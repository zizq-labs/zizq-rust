# Installation & Setup

> [!NOTE]
> If you have not yet installed the Zizq server, follow the
> [Getting Started](/docs/getting-started/) guide first.

The official Zizq Rust client is the [`zizq`](https://crates.io/crates/zizq)
crate. Add it with Cargo:

> Command:
>
> ```bash
> $ cargo add zizq
> ```

The client's minimum supported Rust version (MSRV) is **1.85**.

## TLS backends

TLS support is behind a feature flag — enable exactly one:

- `rustls-tls` (the **default**) — pure Rust, no system OpenSSL dependency.
- `native-tls` — uses the platform's native TLS library.

To use the native backend instead of the default:

> Cargo.toml:
>
> ```toml
> [dependencies]
> zizq = { version = "0.3", default-features = false, features = ["native-tls"] }
> ```

## Versioning

Zizq client libraries are versioned with the same version numbers as the Zizq
server, which follows the SemVer structure `[MAJOR].[MINOR].[PATCH]`.

Whenever a new version of the server is released, client libraries with the
same major and minor version numbers are also released. Provided the major
versions match, the client should generally have an equal or lower minor
version than the server. Patch numbers are insignificant.

The client should never exceed the server's major and minor version, because
it likely expects functionality that does not exist on the server.

Server Version | Client Version | Supported
---------------|----------------|----------
0.1.0          | 0.1.0          | ✅
0.5.12         | 0.3.7          | ✅
1.0.1          | 0.12.2         | ❌
0.5.12         | 0.5.23         | ✅
0.5.12         | 0.6.0          | ❌

## Configuration

The client is configured by constructing a `Client` with `Client::builder()`.
There is no global configuration object.

> [!TIP]
> The best-practice approach is to read these values from environment
> variables. Examples here are hard-coded for clarity.

> Rust:
>
> ```rust
> use std::time::Duration;
> use zizq::{Client, Format};
> 
> let client = Client::builder()
>     .url("https://host.your.network:7890")
>     .format(Format::Json)
>     .connect_timeout(Duration::from_secs(5))
>     .build()?;
> # Ok::<(), zizq::ZizqError>(())
> ```

The `Client` is internally `Arc`-backed, so cloning is cheap — clone it freely
to share across tasks.

### Options

<table>
    <thead>
        <tr><th>Method</th><th>Description</th></tr>
    </thead>
    <tbody>
        <tr>
            <td><code>url</code></td>
            <td>The base URL of the Zizq server. <strong>Required.</strong></td>
        </tr>
        <tr>
            <td><code>format</code></td>
            <td>
                The wire format between client and server — <code>Format::Json</code>
                or <code>Format::MessagePack</code>. Defaults to
                <code>MessagePack</code>. Both formats are mutually
                compatible.
            </td>
        </tr>
        <tr>
            <td><code>connect_timeout</code></td>
            <td>How long a TCP dial may take before being abandoned. Default 10s.</td>
        </tr>
        <tr>
            <td><code>read_timeout</code></td>
            <td>
                Maximum time between consecutive bytes received. Reset by each
                chunk, so heartbeats on long-lived streams keep it fresh.
                Default 30s.
            </td>
        </tr>
        <tr>
            <td><code>ca_certificate</code></td>
            <td>
                A PEM-encoded root CA certificate to trust, in addition to the
                system roots — for an <code>https://</code> server whose
                certificate is signed by a private CA.
            </td>
        </tr>
        <tr>
            <td><code>client_identity</code></td>
            <td>
                A PEM client certificate and private key, for mutual TLS
                (mTLS).
            </td>
        </tr>
    </tbody>
</table>

### Connecting over TLS

For an `https://` server, supply a custom CA if its certificate is privately
signed, and a client identity if the server requires mutual TLS:

> Rust:
>
> ```rust
> use zizq::Client;
> 
> let client = Client::builder()
>     .url("https://host.your.network:7890")
>     .ca_certificate(std::fs::read("server-ca.pem")?)
>     .client_identity(
>         std::fs::read("client-cert.pem")?,
>         std::fs::read("client-key.pem")?,
>     )
>     .build()?;
> # Ok::<(), Box<dyn std::error::Error>>(())
> ```

> [!CAUTION]
> If your server is exposed directly to the internet, it should require
> mutual TLS — otherwise anybody can communicate with it.

The `ca_certificate` and `client_identity` methods are available only when a
TLS feature is enabled (`rustls-tls`, the default, or `native-tls`).
