//! Real native queues with a synchronous loopback service. No mocked frame I/O.

use core::cell::RefCell;
use kernel_api::model::{
    clone_view::CloneLimits,
    identity::{Mark, PendId},
    unit::{UnitQuery, UnitTarget},
};
use kernel_client::call::{clone_view, pend};
use twine::view::{
    clone::{Receiver, Reply, Sender},
    memory::Memory,
};
use weave::{Contract, FrameSender, request_reply, server, transport};

use crate::fixture::{context, inspect, retry, space};

struct Exchange;

impl Contract for Exchange {
    type Request = u32;
    type Response = u32;
}

// The fixture runs the real receiving service before returning from send;
// scheduling is intentionally not under test. The reply grant is explicitly
// prepared by the caller and passed as fixture configuration, not inferred
// from an unauthenticated request field.
struct Service<'a, M: Memory> {
    sender: Sender,
    receiver: &'a mut Receiver<M>,
    reply: PendId,
    calls: usize,
}

impl<M: Memory> FrameSender for Service<'_, M> {
    type Error = ();

    fn send(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.sender.send(bytes).expect("native request submission");
        let reply = self.reply;
        let calls = &mut self.calls;
        server::serve_once::<Exchange, _, _, _, _>(
            self.receiver,
            |_, source| Sender::reply(reply, source),
            |request| {
                *calls += 1;
                assert_eq!(request, 42);
                Ok::<_, ()>(request + 1)
            },
            &mut [0; 4],
            &mut [0; 4],
        )
        .expect("native decode, route, dispatch and reply");
        Ok(())
    }
}

pub fn run(license: PendId) {
    let UnitTarget::Task(caller) = inspect(UnitQuery::Caller).target else {
        panic!("caller");
    };
    let memory = RefCell::new(context(space()));
    let queue = clone_view::create(
        license,
        Mark::NONE,
        CloneLimits {
            max_len: 4,
            max_messages: 1,
            max_bytes: 4,
        },
    )
    .unwrap();
    let mut receiver = Receiver::new(queue, &memory);

    // Both reply routes belong to the same caller/service identity. A
    // delayed reply to the older route must not become the new call's reply.
    let mut old = Reply::create(license, 4, &memory).unwrap();
    let old_token = old.grant(caller).unwrap();
    let mut fresh = Reply::create(license, 4, &memory).unwrap();
    let fresh_token = fresh.grant(caller).unwrap();
    assert_ne!(old_token, fresh_token);
    let mut old_sender = Sender::reply(old_token, caller).unwrap();
    transport::send(&mut old_sender, &99u32, &mut [0; 4]).unwrap();
    assert!(transport::receive::<u32, _>(&mut fresh, &mut [0; 4]).is_err());

    {
        let mut service = Service {
            sender: Sender::new(queue),
            receiver: &mut receiver,
            reply: fresh_token,
            calls: 0,
        };
        let received = request_reply::call::<Exchange, _, _>(
            &mut service,
            &mut fresh,
            &42,
            &mut [0; 4],
            &mut [0; 4],
        )
        .unwrap();
        assert_eq!(received.value, 43);
        assert_eq!(received.metadata, caller);
        assert_eq!(service.calls, 1);
    }

    let delayed = transport::receive::<u32, _>(&mut old, &mut [0; 4]).unwrap();
    assert_eq!(delayed.value, 99);
    assert_eq!(delayed.metadata, caller);
    assert!(transport::receive::<u32, _>(&mut fresh, &mut [0; 4]).is_err());
    assert!(transport::receive::<u32, _>(&mut old, &mut [0; 4]).is_err());

    // Explicitly settle both reply routes and the native request delivery.
    retry(|| fresh.finish());
    retry(|| old.finish());
    retry(|| receiver.settle());
    drop(receiver);
    clone_view::delete(queue).unwrap();
    retry(|| pend::return_(queue));
}
