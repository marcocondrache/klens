use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use url::{Host, Url};

const ID_PREFIX: &str = "klens_client_";

/// A client picks its own name, and the name lands on the consent page and on
/// log lines.
const MAX_NAME_CHARS: usize = 64;

/// Keeps the client id short enough for the query of an authorize URL.
const MAX_REDIRECT_URIS: usize = 8;
const MAX_REDIRECT_URI_BYTES: usize = 256;

/// Desktop apps that the OS hands a URI of their own scheme to.
const APP_SCHEMES: [&str; 4] = ["cursor", "vscode", "vscode-insiders", "jetbrains"];

/// Hosted clients finish a sign-in on their own servers. Any other `https`
/// redirect would send the code to whichever site started the sign-in, once a
/// person approved it by mistake.
const HOSTED_CALLBACKS: [&str; 5] = [
    "https://claude.ai/api/mcp/auth_callback",
    "https://claude.com/api/mcp/auth_callback",
    "https://chatgpt.com/connector_platform_oauth_redirect",
    "https://vscode.dev/redirect",
    "https://insiders.vscode.dev/redirect",
];

/// A registered client, carried whole in its id.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Client {
    #[serde(rename = "n", default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "r")]
    pub redirect_uris: Vec<String>,
}

impl Client {
    /// Keeps the redirect URIs klens takes and drops the rest, since a client
    /// such as VS Code registers a loopback URI and a hosted one together.
    /// `None` when none is left.
    pub(super) fn register(name: Option<&str>, redirect_uris: &[String]) -> Option<Self> {
        let mut kept: Vec<String> = Vec::new();
        for uri in redirect_uris.iter().filter(|uri| allowed(uri)) {
            if kept.len() == MAX_REDIRECT_URIS {
                break;
            }
            if !kept.contains(uri) {
                kept.push(uri.clone());
            }
        }
        if kept.is_empty() {
            return None;
        }
        let name = name
            .map(|name| {
                name.chars()
                    .filter(|c| !c.is_control())
                    .take(MAX_NAME_CHARS)
                    .collect::<String>()
                    .trim()
                    .to_owned()
            })
            .filter(|name| !name.is_empty());
        Some(Self {
            name,
            redirect_uris: kept,
        })
    }

    /// Registration is open, so a MAC would prove only that someone asked
    /// klens for this id, and a key made at boot would void every id at a
    /// restart.
    pub(super) fn id(&self) -> String {
        let json = serde_json::to_vec(self).expect("a client serializes");
        format!("{ID_PREFIX}{}", URL_SAFE_NO_PAD.encode(json))
    }

    /// Anyone can write an id by hand, so every rule of [`Client::register`]
    /// applies again.
    pub(super) fn decode(id: &str) -> Option<Self> {
        let json = URL_SAFE_NO_PAD.decode(id.strip_prefix(ID_PREFIX)?).ok()?;
        let client: Self = serde_json::from_slice(&json).ok()?;
        let registered = Self::register(client.name.as_deref(), &client.redirect_uris)?;
        (registered == client).then_some(client)
    }

    /// The requested redirect, when it is a registered one. RFC 8252 7.3: a
    /// loopback redirect matches whatever port the app listens on this time.
    /// Every other redirect matches exactly.
    pub(super) fn redirect(&self, requested: &str) -> Option<Url> {
        self.redirect_uris
            .iter()
            .any(|registered| registered == requested || same_but_port(registered, requested))
            .then(|| Url::parse(requested).ok())
            .flatten()
    }
}

/// A registered `http` redirect passed [`allowed`], so it is a loopback one.
fn same_but_port(registered: &str, requested: &str) -> bool {
    let (Ok(mut registered), Ok(mut requested)) = (Url::parse(registered), Url::parse(requested))
    else {
        return false;
    };
    if registered.scheme() != "http" {
        return false;
    }
    let ports = (registered.set_port(None), requested.set_port(None));
    ports.0.is_ok() && ports.1.is_ok() && registered == requested
}

fn allowed(uri: &str) -> bool {
    if uri.len() > MAX_REDIRECT_URI_BYTES {
        return false;
    }
    let Ok(url) = Url::parse(uri) else {
        return false;
    };
    if url.fragment().is_some() || !url.username().is_empty() || url.password().is_some() {
        return false;
    }
    match url.scheme() {
        "http" => is_loopback(&url),
        "https" => HOSTED_CALLBACKS.contains(&uri),
        scheme => APP_SCHEMES.contains(&scheme),
    }
}

/// RFC 8252 7.3: a native app listens on a loopback port of its choosing.
fn is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        Some(Host::Domain(domain)) => domain == "localhost",
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redirect_uris_on_loopback_an_app_scheme_or_a_hosted_callback_are_allowed() {
        for uri in [
            "http://localhost:54321/callback",
            "http://127.0.0.1:33418/",
            "http://[::1]:8080/cb?x=1",
            "http://localhost/callback",
            "cursor://anysphere.cursor-retrieval/oauth/user-abc/callback",
            "vscode://vscode.github-authentication/did-authenticate",
            "jetbrains://gateway/callback",
            "https://claude.ai/api/mcp/auth_callback",
            "https://vscode.dev/redirect",
        ] {
            assert!(allowed(uri), "{uri}");
        }
    }

    #[test]
    fn other_redirect_uris_are_refused() {
        for uri in [
            "http://klens.example.com/callback",
            "http://localhost.evil.example/callback",
            "http://127.0.0.1.nip.io/callback",
            "https://localhost:54321/callback",
            "https://claude.ai/api/mcp/auth_callback/",
            "https://claude.ai/api/mcp/auth_callback?next=x",
            "https://CLAUDE.ai/api/mcp/auth_callback",
            "https://evil.example/api/mcp/auth_callback",
            "http://localhost:54321/callback#fragment",
            "http://user@localhost:54321/callback",
            "http://:secret@localhost:54321/callback",
            "javascript:alert(1)",
            "data:text/html,hi",
            "file:///etc/passwd",
            "custom://callback",
            "/callback",
            "",
        ] {
            assert!(!allowed(uri), "{uri}");
        }
    }

    #[test]
    fn a_redirect_uri_may_take_up_to_the_byte_cap() {
        let prefix = "http://localhost/";
        let longest = format!(
            "{prefix}{}",
            "a".repeat(MAX_REDIRECT_URI_BYTES - prefix.len())
        );

        assert!(allowed(&longest));
        assert!(!allowed(&format!("{longest}a")));
    }

    #[test]
    fn a_registration_keeps_the_allowed_uris_once_each_up_to_the_cap() {
        let mut uris = vec![
            "https://evil.example/callback".to_owned(),
            "http://127.0.0.1:33418/".to_owned(),
            "http://127.0.0.1:33418/".to_owned(),
            "https://vscode.dev/redirect".to_owned(),
        ];
        uris.extend((0..20).map(|port| format!("http://localhost:{port}/")));

        let client = Client::register(Some("Visual Studio Code"), &uris).expect("a client");

        assert_eq!(client.redirect_uris.len(), MAX_REDIRECT_URIS);
        assert_eq!(
            client.redirect_uris[..3],
            [
                "http://127.0.0.1:33418/",
                "https://vscode.dev/redirect",
                "http://localhost:0/"
            ]
        );
    }

    #[test]
    fn a_registration_with_no_allowed_uri_is_refused() {
        let uris = ["https://evil.example/callback".to_owned()];

        assert_eq!(Client::register(Some("evil"), &uris), None);
        assert_eq!(Client::register(None, &[]), None);
    }

    #[test]
    fn a_name_loses_control_characters_and_is_cut() {
        let uris = ["http://localhost/".to_owned()];
        let name = format!(" Claude\u{1b}[2J\nCode{}", "x".repeat(100));

        let client = Client::register(Some(&name), &uris).expect("a client");

        let kept = client.name.expect("a name");
        assert!(kept.starts_with("Claude[2JCode"), "{kept}");
        assert_eq!(kept.chars().count(), MAX_NAME_CHARS - 1);
        let blank = Client::register(Some(" \n "), &uris).expect("a client");
        assert_eq!(blank.name, None);
    }

    #[test]
    fn only_a_loopback_redirect_may_name_another_port() {
        let uris = [
            "http://127.0.0.1:33418/callback".to_owned(),
            "https://vscode.dev/redirect".to_owned(),
            "cursor://anysphere.cursor-retrieval/callback".to_owned(),
        ];
        let client = Client::register(None, &uris).expect("a client");

        let moved = client.redirect("http://127.0.0.1:60000/callback");

        assert_eq!(
            moved.as_ref().map(Url::as_str),
            Some("http://127.0.0.1:60000/callback")
        );
        for refused in [
            "http://127.0.0.1:60000/other",
            "http://localhost:33418/callback",
            "https://vscode.dev:8443/redirect",
            "cursor://anysphere.cursor-retrieval:1/callback",
        ] {
            assert_eq!(client.redirect(refused), None, "{refused}");
        }
    }

    #[test]
    fn the_id_carries_the_registration() {
        let client = Client::register(
            Some("Claude Code"),
            &["http://localhost:54321/callback".to_owned()],
        )
        .expect("a client");

        let id = client.id();

        let encoded = id.strip_prefix(ID_PREFIX).expect("the prefix");
        let decoded: Client =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded).expect("base64"))
                .expect("json");
        assert_eq!(decoded, client);
    }
}
