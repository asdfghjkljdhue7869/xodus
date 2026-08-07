use tokio::io;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

#[cfg(target_os = "linux")]
fn get_runtime_dir() -> String {
    std::env::var("XDG_RUNTIME_DIR").expect("Runtime dir not set")
}

#[cfg(target_os = "macos")]
fn get_runtime_dir() -> String {
    "/tmp/".to_string()
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let runtime_dir = get_runtime_dir();
    let socket_path = format!("{runtime_dir}/xodus.sock");

    let stream = UnixStream::connect(&socket_path).await?;
    let (mut socket_read, mut socket_write) = stream.into_split();

    let mut stdin_to_socket = tokio::spawn(async move {
        let mut stdin = io::stdin();
        io::copy(&mut stdin, &mut socket_write).await?;
        socket_write.shutdown().await
    });

    let mut socket_to_stdout = tokio::spawn(async move {
        let mut stdout = io::stdout();
        io::copy(&mut socket_read, &mut stdout).await?;
        stdout.shutdown().await
    });

    tokio::select! {
        res = &mut stdin_to_socket => res.expect("stdin forwarding task panicked")?,
        res = &mut socket_to_stdout => res.expect("stdout forwarding task panicked")?,
    }

    Ok(())
}
