// use futures::{StreamExt, TryStreamExt};

use super::{BcCamera, Error, Result};
use crate::bc::{model::*, xml::*};

#[cfg(test)]
use crate::bc_protocol::connection::mock::{reply_err_code, MockConnection};

impl BcCamera {
	/// Get the snapshot image
	pub async fn get_snapshot(&self) -> Result<Vec<u8>> {
		// Gate on the `preview` ability — every Reolink camera that
		// produces a snapshot also advertises preview (the snap path
		// pulls a JPEG out of the same imager pipeline). Without this
		// gate, a `MissingAbility` outcome surfaces as the generic
		// `CameraServiceUnavailable` and the CLI exit code maps to 5
		// (protocol) instead of the more accurate 6 (unsupported).
		self.has_ability_ro("preview").await?;
		// Serialize snapshot fetches on this camera: `subscribe_to_id`
		// keys its wildcard slot on `(MSG_ID_SNAP, None)` until the
		// camera's first chunk reveals the real msg_num. Two concurrent
		// `get_snapshot` calls (e.g. the preview poller tick racing an
		// on-demand MQTT/RTSP-triggered snapshot) both take that slot,
		// and the second silently replaces the first, splitting the
		// binary chunks between the two callers. Holding this lock for
		// the whole call closes that race. See `snap_lock` doc comment.
		let _snap_guard = self.snap_lock.lock().await;
		let connection = self.get_connection();
		let msg_num = self.new_message_num();
		let mut sub_get = connection.subscribe(MSG_ID_SNAP, msg_num).await?;
		let get = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_SNAP,
				channel_id: self.channel_id,
				msg_num,
				response_code: 0,
				stream_type: 0,
				class: 0x6414,
			},
			body: BcBody::ModernMsg(ModernMsg {
				extension: Some(Extension {
					channel_id: Some(self.channel_id),
					..Default::default()
				}),
				payload: Some(BcPayloads::BcXml(BcXml {
					snap: Some(Snap {
						version: "1.1".to_string(),
						channel_id: self.channel_id,
						logic_channel: Some(self.channel_id),
						time: 0,
						full_frame: Some(0),
						stream_type: Some("main".to_string()),
						..Default::default()
					}),
					..Default::default()
				})),
			}),
		};

		sub_get.send(get).await?;
		let msg = sub_get.recv().await?;
		if msg.meta.response_code != 200 {
			return Err(Error::CameraServiceUnavailable {
				id: msg.meta.msg_id,
				code: msg.meta.response_code,
			});
		}

		if let BcBody::ModernMsg(ModernMsg {
			payload:
				Some(BcPayloads::BcXml(BcXml {
					snap:
						Some(Snap {
							file_name: Some(filename),
							picture_size: Some(expected_size),
							..
						}),
					..
				})),
			..
		}) = msg.body
		{
			log::trace!("Got snap XML {} with size {}", filename, expected_size);
			// Messages are now sent on ID 109 but not with the same message ID
			// preumably because the camera considers it to be a new message rather
			// than a reply
			//
			// This means we need to listen for the next 109 grab the message num and
			// subscribe to it. This is what `subscribe_to_next` is for.
			//
			// Race-window note: install the wildcard `subscribe_to_id`
			// subscriber BEFORE dropping the msg_num-scoped one. The
			// camera fires its binary chunks on a fresh msg_num right
			// after the XML reply; a `drop` → `subscribe_to_id` ordering
			// drops every chunk that lands in between because no
			// subscriber matches. With the wildcard installed first,
			// chunks land on the `None` subscriber and the dispatcher's
			// "upgrade None → Some(msg_num)" path takes over.
			//
			// Hard cap on the snapshot size: the loop below appends every
			// 200-coded chunk to `result` and only terminates on a non-200
			// response_code or connection drop. Without this cap, a buggy
			// or malicious camera reply can OOM the process. 16 MiB is
			// well above any 4K JPEG observed on Argus hardware.
			const MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
			if (expected_size as usize) > MAX_SNAPSHOT_BYTES {
				return Err(Error::CameraServiceUnavailable {
					id: MSG_ID_SNAP,
					code: 200,
				});
			}
			let mut chunk_sub = connection.subscribe_to_id(MSG_ID_SNAP).await?;
			drop(sub_get);
			let expected_size = expected_size as usize;

			let mut result: Vec<u8> = Vec::with_capacity(expected_size);
			log::trace!("Waiting for packets on {}", msg_num);
			let mut msg = chunk_sub.recv().await?;

			while msg.meta.response_code == 200 {
				// sends 200 while more is to come
				//       201 when finished

				if let BcBody::ModernMsg(ModernMsg {
					extension: Some(Extension {
						binary_data: Some(1),
						..
					}),
					payload: Some(BcPayloads::Binary(data)),
				}) = msg.body
				{
					result.extend_from_slice(&data);
					if result.len() > MAX_SNAPSHOT_BYTES {
						return Err(Error::CameraServiceUnavailable {
							id: MSG_ID_SNAP,
							code: 200,
						});
					}
				} else {
					return Err(Error::UnintelligibleReply {
						reply: std::sync::Arc::new(msg),
						why: "Expected binary data but got something else",
					});
				}
				log::trace!(
					"Got packet size is now {} of {}",
					result.len(),
					expected_size
				);
				msg = chunk_sub.recv().await?;
			}

			if msg.meta.response_code == 201 {
				// 201 means all binary data sent
				if let BcBody::ModernMsg(ModernMsg {
					extension: Some(Extension {
						binary_data: Some(1),
						..
					}),
					payload,
				}) = msg.body
				{
					if let Some(BcPayloads::Binary(data)) = payload {
						// Add last data if present (may be zero if previous packet contained it)
						result.extend_from_slice(&data);
					}
					log::trace!(
						"Got all packets size is now {} of {}",
						result.len(),
						expected_size
					);
				} else {
					return Err(Error::UnintelligibleReply {
						reply: std::sync::Arc::new(msg),
						why: "Expected binary data but got something else",
					});
				}
			} else {
				// anything else is an error
				return Err(Error::CameraServiceUnavailable {
					id: msg.meta.msg_id,
					code: msg.meta.response_code,
				});
			}

			log::trace!("Snapshot received: {} of {}", result.len(), expected_size);

			// A truncated snapshot is a real bug, not a tracing nicety.
			// Surface it so callers (oneshot snapshot, MQTT preview,
			// startup-wake) can fail-loud rather than write a half-JPEG
			// to disk / publish a torn frame to HA.
			if result.len() != expected_size {
				log::error!(
					"Snap truncated: got {} bytes, expected {}",
					result.len(),
					expected_size
				);
				return Err(Error::UnintelligibleReply {
					reply: std::sync::Arc::new(Bc {
						meta: BcMeta {
							msg_id: MSG_ID_SNAP,
							channel_id: self.channel_id,
							msg_num,
							stream_type: 0,
							response_code: 201,
							class: 0x6414,
						},
						body: BcBody::ModernMsg(ModernMsg {
							extension: None,
							payload: None,
						}),
					}),
					why: "Snap truncated: chunk total did not match advertised picture_size",
				});
			}

			Ok(result)
		} else {
			Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "Expected Snap xml but it was not received",
			})
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// Snap's happy path issues a second `subscribe_to_id(MSG_ID_SNAP)`
	// and streams binary chunks on arbitrary msg_nums — out of scope
	// for the in-order scripted `MockConnection` harness. Error paths
	// (non-200 on the initial XML + missing-ability upstream) are
	// covered here; the full binary-chunk path lives in live-fire
	// tests on real hardware.
	#[tokio::test]
	async fn get_snapshot_missing_ability_returns_err() {
		// Fresh camera with no abilities populated → the new
		// `has_ability_ro("preview")` gate must short-circuit with
		// MissingAbility (exit code 6 "unsupported"), not the generic
		// protocol-error fallthrough.
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		let err = cam.get_snapshot().await.expect_err("should fail");
		assert!(matches!(err, Error::MissingAbility { .. }));
	}

	#[tokio::test]
	async fn get_snapshot_non_200_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SNAP)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("preview", false).await;
		let err = cam.get_snapshot().await.expect_err("should fail");
		assert!(matches!(
			err,
			Error::CameraServiceUnavailable { code: 500, .. }
		));
	}

	#[tokio::test]
	async fn get_snapshot_missing_snap_xml_returns_err() {
		// 200 OK but no `snap` xml in the payload -> UnintelligibleReply.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SNAP)
			.reply_with(|req| {
				crate::bc_protocol::connection::mock::reply_200_xml(req, BcXml::default())
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("preview", false).await;
		let err = cam.get_snapshot().await.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	/// Build a synthetic binary-chunk reply on `MSG_ID_SNAP`.
	fn binary_chunk(msg_num: u16, code: u16, data: Vec<u8>) -> Bc {
		Bc {
			meta: BcMeta {
				msg_id: MSG_ID_SNAP,
				channel_id: 0,
				msg_num,
				stream_type: 0,
				response_code: code,
				class: 0x6414,
			},
			body: BcBody::ModernMsg(ModernMsg {
				extension: Some(Extension {
					binary_data: Some(1),
					..Default::default()
				}),
				payload: Some(BcPayloads::Binary(data)),
			}),
		}
	}

	#[tokio::test]
	async fn get_snapshot_happy_path_multi_chunk() {
		// 3 chunks totalling 6 bytes → one 200 chunk, then the 201
		// terminator with the last chunk.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SNAP)
			.reply_with(|req| {
				crate::bc_protocol::connection::mock::reply_200_xml(
					req,
					BcXml {
						snap: Some(Snap {
							version: "1.1".to_string(),
							channel_id: 0,
							logic_channel: Some(0),
							time: 0,
							full_frame: Some(0),
							stream_type: Some("main".to_string()),
							file_name: Some("snap.jpg".to_string()),
							picture_size: Some(6),
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let injector = mock.injector();
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("preview", false).await;

		// Race: the client sends the initial request, then drops the
		// msg_num-scoped subscriber and `subscribe_to_id`s. Inject the
		// chunks after a short yield loop so the `AddSubscriber`
		// PollCommand lands first.
		let injector_task = tokio::spawn(async move {
			tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
			// Two chunks with the same msg_num so the `subscribe_to_id`
			// "upgrade None → Some(msg_num)" path delivers both.
			injector
				.push(binary_chunk(42, 200, vec![0xDE, 0xAD, 0xBE]))
				.await;
			injector
				.push(binary_chunk(42, 201, vec![0xEF, 0xCA, 0xFE]))
				.await;
		});

		let bytes =
			tokio::time::timeout(tokio::time::Duration::from_millis(500), cam.get_snapshot())
				.await
				.expect("did not hang")
				.expect("ok");
		assert_eq!(bytes, vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE]);
		injector_task.await.ok();
	}

	#[tokio::test]
	async fn get_snapshot_terminator_not_201_returns_err() {
		// Second chunk has code 500 — not 200 (more) and not 201
		// (done) → CameraServiceUnavailable.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SNAP)
			.reply_with(|req| {
				crate::bc_protocol::connection::mock::reply_200_xml(
					req,
					BcXml {
						snap: Some(Snap {
							version: "1.1".to_string(),
							channel_id: 0,
							logic_channel: Some(0),
							time: 0,
							full_frame: Some(0),
							stream_type: Some("main".to_string()),
							file_name: Some("snap.jpg".to_string()),
							picture_size: Some(4),
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let injector = mock.injector();
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("preview", false).await;

		tokio::spawn(async move {
			tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
			injector.push(binary_chunk(42, 500, vec![])).await;
		});
		let err = tokio::time::timeout(tokio::time::Duration::from_millis(500), cam.get_snapshot())
			.await
			.expect("did not hang")
			.expect_err("should fail");
		assert!(matches!(
			err,
			Error::CameraServiceUnavailable { code: 500, .. }
		));
	}

	#[tokio::test]
	async fn get_snapshot_oversized_picture_size_is_rejected_upfront() {
		// Camera advertises a picture_size > 16 MiB (MAX_SNAPSHOT_BYTES).
		// We must reject before opening the binary-chunk subscription so
		// a malicious or buggy camera reply can never OOM the process.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SNAP)
			.reply_with(|req| {
				crate::bc_protocol::connection::mock::reply_200_xml(
					req,
					BcXml {
						snap: Some(Snap {
							version: "1.1".to_string(),
							channel_id: 0,
							logic_channel: Some(0),
							time: 0,
							full_frame: Some(0),
							stream_type: Some("main".to_string()),
							file_name: Some("huge.jpg".to_string()),
							picture_size: Some(32 * 1024 * 1024),
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("preview", false).await;
		let err = cam.get_snapshot().await.expect_err("should fail");
		assert!(matches!(
			err,
			Error::CameraServiceUnavailable {
				id: MSG_ID_SNAP,
				code: 200,
			}
		));
	}

	#[tokio::test]
	async fn get_snapshot_terminator_missing_binary_returns_err() {
		// 201 terminator with no binary_data extension → UnintelligibleReply.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SNAP)
			.reply_with(|req| {
				crate::bc_protocol::connection::mock::reply_200_xml(
					req,
					BcXml {
						snap: Some(Snap {
							version: "1.1".to_string(),
							channel_id: 0,
							logic_channel: Some(0),
							time: 0,
							full_frame: Some(0),
							stream_type: Some("main".to_string()),
							file_name: Some("snap.jpg".to_string()),
							picture_size: Some(4),
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let injector = mock.injector();
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("preview", false).await;

		tokio::spawn(async move {
			tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
			// 201 terminator without `binary_data: Some(1)` extension →
			// UnintelligibleReply.
			injector
				.push(Bc {
					meta: BcMeta {
						msg_id: MSG_ID_SNAP,
						channel_id: 0,
						msg_num: 42,
						stream_type: 0,
						response_code: 201,
						class: 0x6414,
					},
					body: BcBody::ModernMsg(ModernMsg {
						extension: None,
						payload: None,
					}),
				})
				.await;
		});
		let err = tokio::time::timeout(tokio::time::Duration::from_millis(500), cam.get_snapshot())
			.await
			.expect("did not hang")
			.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	/// Truncated snapshot (chunk total < advertised picture_size)
	/// must surface as `UnintelligibleReply` so callers fail loud
	/// rather than write a half-JPEG to disk.
	#[tokio::test]
	async fn get_snapshot_truncated_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SNAP)
			.reply_with(|req| {
				crate::bc_protocol::connection::mock::reply_200_xml(
					req,
					BcXml {
						snap: Some(Snap {
							version: "1.1".to_string(),
							channel_id: 0,
							logic_channel: Some(0),
							time: 0,
							full_frame: Some(0),
							stream_type: Some("main".to_string()),
							file_name: Some("snap.jpg".to_string()),
							picture_size: Some(8),
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let injector = mock.injector();
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("preview", false).await;

		// Camera advertises 8 bytes but only 4 arrive across the
		// 200/201 sequence.
		tokio::spawn(async move {
			tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
			injector.push(binary_chunk(42, 200, vec![0xAA, 0xBB])).await;
			injector.push(binary_chunk(42, 201, vec![0xCC, 0xDD])).await;
		});
		let err = tokio::time::timeout(tokio::time::Duration::from_millis(500), cam.get_snapshot())
			.await
			.expect("did not hang")
			.expect_err("must surface truncation");
		assert!(
			matches!(err, Error::UnintelligibleReply { .. }),
			"truncated snapshot should be UnintelligibleReply, got {err:?}"
		);
	}

	/// Regression test for the `snap_lock` fix: two `get_snapshot`
	/// calls firing concurrently on the same camera (e.g. the preview
	/// poller tick racing an on-demand MQTT/RTSP-triggered snapshot)
	/// must not cross-deliver binary chunks. Before the fix, both
	/// calls raced to occupy the single `(MSG_ID_SNAP, None)` wildcard
	/// subscriber slot in the poller and the second silently stole the
	/// first's chunks, surfacing as "Snap truncated". With the lock
	/// serialising the whole call, the second request isn't even sent
	/// until the first has fully drained its chunks, so both
	/// snapshots must come back complete and un-mixed.
	#[tokio::test]
	async fn get_snapshot_concurrent_calls_do_not_cross_deliver_chunks() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SNAP)
			.reply_with(|req| {
				crate::bc_protocol::connection::mock::reply_200_xml(
					req,
					BcXml {
						snap: Some(Snap {
							version: "1.1".to_string(),
							channel_id: 0,
							logic_channel: Some(0),
							time: 0,
							full_frame: Some(0),
							stream_type: Some("main".to_string()),
							file_name: Some("a.jpg".to_string()),
							picture_size: Some(3),
						}),
						..Default::default()
					},
				)
			})
			.expect_msg(MSG_ID_SNAP)
			.reply_with(|req| {
				crate::bc_protocol::connection::mock::reply_200_xml(
					req,
					BcXml {
						snap: Some(Snap {
							version: "1.1".to_string(),
							channel_id: 0,
							logic_channel: Some(0),
							time: 0,
							full_frame: Some(0),
							stream_type: Some("main".to_string()),
							file_name: Some("b.jpg".to_string()),
							picture_size: Some(3),
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let injector = mock.injector();
		let cam = std::sync::Arc::new(BcCamera::from_mock_connection(mock).await);
		cam.test_set_ability("preview", false).await;

		let cam_a = cam.clone();
		let task_a = tokio::spawn(async move { cam_a.get_snapshot().await });
		let cam_b = cam.clone();
		let task_b = tokio::spawn(async move { cam_b.get_snapshot().await });

		// The lock forces request A's XML round trip (and its whole
		// chunk sequence) to complete before B's request is even sent,
		// so injecting A's chunks first, then giving B time to send
		// its request and subscribe, then injecting B's chunks, is
		// deterministic regardless of which task wins the lock race.
		tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
		injector.push(binary_chunk(42, 200, vec![0xAA])).await;
		injector
			.push(binary_chunk(42, 201, vec![0xBB, 0xCC]))
			.await;
		tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
		injector.push(binary_chunk(43, 200, vec![0x11])).await;
		injector
			.push(binary_chunk(43, 201, vec![0x22, 0x33]))
			.await;

		let (res_a, res_b) = tokio::time::timeout(
			tokio::time::Duration::from_millis(1000),
			futures::future::join(task_a, task_b),
		)
		.await
		.expect("did not hang");
		let bytes_a = res_a.expect("join ok").expect("snapshot ok");
		let bytes_b = res_b.expect("join ok").expect("snapshot ok");

		let mut results = vec![bytes_a, bytes_b];
		results.sort();
		assert_eq!(
			results,
			vec![vec![0x11, 0x22, 0x33], vec![0xAA, 0xBB, 0xCC]],
			"each snapshot must receive exactly its own chunks, un-mixed"
		);
	}

	#[tokio::test]
	async fn get_snapshot_chunk_missing_binary_returns_err() {
		// First chunk arrives with response_code 200 but no binary
		// payload → UnintelligibleReply.
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_SNAP)
			.reply_with(|req| {
				crate::bc_protocol::connection::mock::reply_200_xml(
					req,
					BcXml {
						snap: Some(Snap {
							version: "1.1".to_string(),
							channel_id: 0,
							logic_channel: Some(0),
							time: 0,
							full_frame: Some(0),
							stream_type: Some("main".to_string()),
							file_name: Some("snap.jpg".to_string()),
							picture_size: Some(4),
						}),
						..Default::default()
					},
				)
			})
			.build()
			.await;
		let injector = mock.injector();
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("preview", false).await;

		tokio::spawn(async move {
			tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
			// 200 with no binary payload — UnintelligibleReply.
			injector
				.push(Bc {
					meta: BcMeta {
						msg_id: MSG_ID_SNAP,
						channel_id: 0,
						msg_num: 42,
						stream_type: 0,
						response_code: 200,
						class: 0x6414,
					},
					body: BcBody::ModernMsg(ModernMsg {
						extension: None,
						payload: None,
					}),
				})
				.await;
		});
		let err = tokio::time::timeout(tokio::time::Duration::from_millis(500), cam.get_snapshot())
			.await
			.expect("did not hang")
			.expect_err("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}
}
