use crate::{bail, ResultType};
use bytes::BytesMut;
use futures::SinkExt;
use protobuf::Message;
use std::{
    io::Error,
    net::SocketAddr,
    ops::{Deref, DerefMut},
};
use tokio::{net::ToSocketAddrs, net::UdpSocket, stream::StreamExt};
use tokio_util::{codec::BytesCodec, udp::UdpFramed};

pub struct FramedSocket(UdpFramed<BytesCodec>);

impl Deref for FramedSocket {
    type Target = UdpFramed<BytesCodec>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FramedSocket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FramedSocket {
    pub async fn new<T: ToSocketAddrs + std::fmt::Debug>(addr: T) -> ResultType<Self> {
        log::info!("New FramedSocket addr {:?}", &addr);
        let socket = UdpSocket::bind(addr).await?;
        Ok(Self(UdpFramed::new(socket, BytesCodec::new())))
    }

    #[allow(clippy::never_loop)]
    pub async fn new_reuse<T: ToSocketAddrs + std::fmt::Debug>(addr: T) -> ResultType<Self> {
        for addr in addr.to_socket_addrs().await? {
            log::info!("new_reuse FramedSocket addr {:?}", addr);
            return Ok(Self(UdpFramed::new(
                UdpSocket::from_std(super::new_socket(addr, false, true)?.into_udp_socket())?,
                BytesCodec::new(),
            )));
        }
        bail!("could not resolve to any address");
    }

    #[inline]
    pub async fn send(&mut self, msg: &impl Message, addr: SocketAddr) -> ResultType<()> {
        log::info!("FramedSocket send {:?}", (&msg, &addr));
        self.0
            .send((bytes::Bytes::from(msg.write_to_bytes().unwrap()), addr))
            .await?;
        Ok(())
    }

    #[inline]
    pub async fn send_raw(&mut self, msg: &'static [u8], addr: SocketAddr) -> ResultType<()> {
        log::info!("FramedSocket send_raw {:?}", (&msg, &addr));
        self.0.send((bytes::Bytes::from(msg), addr)).await?;
        Ok(())
    }

    #[inline]
    pub async fn next(&mut self) -> Option<Result<(BytesMut, SocketAddr), Error>> {
        let x = self.0.next().await;
        log::info!("FramedSocket next {:?}", (&x));
        x
    }

    #[inline]
    pub async fn next_timeout(&mut self, ms: u64) -> Option<Result<(BytesMut, SocketAddr), Error>> {
        if let Ok(res) =
            tokio::time::timeout(std::time::Duration::from_millis(ms), self.0.next()).await
        {
            log::info!("FramedSocket next_timeout {:?}", res);
            res
        } else {
            None
        }
    }
}
