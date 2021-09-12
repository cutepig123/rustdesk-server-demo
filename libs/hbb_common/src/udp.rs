use crate::{ResultType};
use bytes::BytesMut;
use bytes::Bytes;
// use futures::{SinkExt, StreamExt};
use protobuf::Message;
use std::{
    io::Error,
    net::SocketAddr,
    ops::{Deref, DerefMut},
};
use tokio::{net::ToSocketAddrs};
//use tokio_util::{codec::BytesCodec, udp::UdpFramed};
use mini_redis::{client};
use serde_derive::{Deserialize, Serialize};

pub struct FramedSocket{
    //UdpFramed<BytesCodec>
    myaddr: SocketAddr,
    client_pub: mini_redis::client::Client,
    subscriber: mini_redis::client::Subscriber,
}

// impl Deref for FramedSocket {
//     type Target = UdpFramed<BytesCodec>;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

// impl DerefMut for FramedSocket {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.0
//     }
// }

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Msg{
    msg: Vec<u8>,
    addr: SocketAddr,
}

impl FramedSocket {
    pub async fn new(addr: SocketAddr) -> ResultType<Self> {
        let client = mini_redis::client::connect("127.0.0.1:6379").await.unwrap();
        let client_pub = mini_redis::client::connect("127.0.0.1:6379").await.unwrap();
        log::info!("New FramedSocket addr {:?}", &addr);
        
        let channel = format!("{}", &addr);
        let subscriber = client.subscribe(vec![channel]).await.unwrap();

        Ok(Self{myaddr: addr, client_pub, subscriber})
    }

    
    pub async fn send(&mut self, msg: &impl Message, addr: SocketAddr) -> ResultType<()> {
        log::info!("FramedSocket send {:?}", (&msg, &addr));

        let msg = Msg{msg:msg.write_to_bytes().unwrap(), addr};
        let encoded: Vec<u8> = bincode::serialize(&msg).unwrap();

        let channel = format!("{}", &addr);
        self.client_pub.publish(&channel, encoded.into()).await.unwrap();

        Ok(())
    }

    pub async fn next(&mut self) -> Option<Result<(BytesMut, SocketAddr), Error>> {
        let encoded = self.subscriber.next_message().await.unwrap().unwrap().content;
        let decoded: Msg = bincode::deserialize(&encoded[..]).unwrap();
        log::info!("FramedSocket next {:?}", (&decoded));
        let msg = decoded.msg;
        let msg = &msg[..];
        Some(Ok((BytesMut::from(msg), decoded.addr)))
    }

    #[inline]
    pub async fn next_timeout(&mut self, ms: u64) -> Option<Result<(BytesMut, SocketAddr), Error>> {
        let x = 
            tokio::time::timeout(std::time::Duration::from_millis(ms), self.next()).await;
        let x = x.expect("msg");
        x
    }
}
