use crate::ResultType;
use bytes::BytesMut;
//use bytes::Bytes;
// use futures::{SinkExt, StreamExt};
use protobuf::Message;
use std::{
    io::Error,
    net::{IpAddr, SocketAddr},
};
//use tokio::{net::ToSocketAddrs};
//use tokio_util::{codec::BytesCodec, udp::UdpFramed};
//use mini_redis::{client};
use serde_derive::{Deserialize, Serialize};

pub struct FramedSocket {
    //UdpFramed<BytesCodec>
    //myaddr: SocketAddr,
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
struct Msg {
    msg: Vec<u8>,
    addr: SocketAddr,
}

impl FramedSocket {
    pub async fn new(listen_addr: SocketAddr) -> ResultType<Self> {
        let redis_server_ip = crate::config::Config2::load()
            .options
            .get("relay-server")
            .unwrap()
            .clone();
        let redis_server = format!("{}:6379", redis_server_ip);
        log::info!(
            "New FramedSocket addr {:?} redis_server {:?} ",
            &listen_addr,
            &redis_server
        );
        if let IpAddr::V4(ip) = listen_addr.ip() {
            assert!(ip.octets()[0] != 0);
        }

        let client = mini_redis::client::connect(&redis_server).await.unwrap();
        let client_pub = mini_redis::client::connect(&redis_server).await.unwrap();

        let channel = format!("{}", &listen_addr);
        log::info!("subscribe to channel {}", &channel);
        let subscriber = client.subscribe(vec![channel]).await.unwrap();

        Ok(Self {
            client_pub,
            subscriber,
        })
    }

    pub async fn send(&mut self, msg: &impl Message, addr: SocketAddr) -> ResultType<()> {
        // Message -> Msg
        log::info!("FramedSocket send Message {:?}", (&msg));
        let msg = Msg {
            msg: msg.write_to_bytes().unwrap(),
            addr,
        };
        log::info!("FramedSocket -> Msg {:?}", (&msg));

        // --> vec --> String -> bytes
        let encoded_vec: Vec<u8> = bincode::serialize(&msg).unwrap();
        let b64 = base64::encode(&encoded_vec);
        let encoded_bytes = bytes::Bytes::from(b64);
        log::info!(
            "FramedSocket encoded vec {:?} -> bytes {:?}",
            &encoded_vec,
            &encoded_bytes
        );

        // --> publish
        let channel = format!("{}", &addr);
        let num_sub = self
            .client_pub
            .publish(&channel, encoded_bytes)
            .await
            .unwrap();
        log::info!("publish channel {} returns {}", &channel, num_sub);

        Ok(())
    }

    pub async fn next(&mut self) -> Option<Result<(BytesMut, SocketAddr), Error>> {
        // Get --> bytes -> b64 -> vec
        let encoded_bytes = self
            .subscriber
            .next_message()
            .await
            .unwrap()
            .unwrap()
            .content;
        let b64 = base64::decode(&encoded_bytes[..]).unwrap();
        let encoded_vec = Vec::from(&b64[..]);
        log::info!(
            "FramedSocket next bytes {:?} --> vec {:?}",
            &encoded_bytes,
            &encoded_vec
        );

        // --> Msg
        let decoded: Msg = bincode::deserialize(&encoded_vec).unwrap();
        log::info!("FramedSocket deserialized {:?}", (&decoded));

        let msg = decoded.msg;
        let msg = &msg[..];
        Some(Ok((BytesMut::from(msg), decoded.addr)))
    }

    #[inline]
    pub async fn next_timeout(&mut self, ms: u64) -> Option<Result<(BytesMut, SocketAddr), Error>> {
        let x = tokio::time::timeout(std::time::Duration::from_millis(ms), self.next()).await;
        let x = x.expect("msg");
        x
    }
}
