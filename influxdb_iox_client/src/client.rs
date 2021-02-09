use std::num::NonZeroU32;

use data_types::database_rules::DatabaseRules;
use reqwest::{Method, Url};

use crate::errors::{ClientError, CreateDatabaseError, Error, ServerErrorResponse};
use data_types::DatabaseName;

// TODO: move DatabaseRules / WriterId to the API client

/// An IOx HTTP API client.
///
/// ```
/// #[tokio::test]
/// # async fn test() {
/// use data_types::database_rules::DatabaseRules;
/// use influxdb_iox_client::ClientBuilder;
///
/// let client = ClientBuilder::default()
///     .build("http://127.0.0.1:8080")
///     .unwrap();
///
/// // Ping the IOx server
/// client.ping().await.expect("server is down :(");
///
/// // Create a new database!
/// client
///     .create_database("bananas", &DatabaseRules::default())
///     .await
///     .expect("failed to create database");
/// # }
/// ```
#[derive(Debug)]
pub struct Client {
    pub(crate) http: reqwest::Client,

    /// The base URL to which request paths are joined.
    ///
    /// A base path of:
    ///
    /// ```text
    ///     https://www.influxdata.com/maybe-proxy/
    /// ```
    ///
    /// Joined with a request path of `/a/reg/` would result in:
    ///
    /// ```text
    ///     https://www.influxdata.com/maybe-proxy/a/req/
    /// ```
    ///
    /// Paths joined to this `base` MUST be relative to be appended to the base
    /// path. Absolute paths joined to `base` are still absolute.
    pub(crate) base: Url,
}

impl std::default::Default for Client {
    fn default() -> Self {
        crate::ClientBuilder::default()
            .build("http://127.0.0.1:8080")
            .expect("default client builder is invalid")
    }
}

impl Client {
    /// Ping the IOx server, checking for a HTTP 200 response.
    pub async fn ping(&self) -> Result<(), Error> {
        const PING_PATH: &str = "ping";

        let r = self
            .http
            .request(Method::GET, self.url_for(PING_PATH))
            .send()
            .await?;

        match r {
            r if r.status() == 200 => Ok(()),
            r => Err(ServerErrorResponse::from_response(r).await.into()),
        }
    }

    /// Creates a new IOx database.
    pub async fn create_database(
        &self,
        name: impl AsRef<str>,
        rules: &DatabaseRules,
    ) -> Result<(), CreateDatabaseError> {
        let url = self.db_url(name.as_ref())?;

        let r = self
            .http
            .request(Method::PUT, url)
            .json(rules)
            .send()
            .await?;

        // Filter out the good states, and convert all others into errors.
        match r {
            r if r.status() == 200 => Ok(()),
            r => Err(ServerErrorResponse::from_response(r).await.into()),
        }
    }

    /// Set the server's writer ID.
    pub async fn set_writer_id(&self, id: NonZeroU32) -> Result<(), Error> {
        const SET_WRITER_PATH: &str = "iox/api/v1/id";

        let url = self.url_for(SET_WRITER_PATH);

        // TODO: move this into a shared type
        #[derive(serde::Serialize)]
        struct WriterIdBody {
            id: u32,
        };

        let r = self
            .http
            .request(Method::PUT, url)
            .json(&WriterIdBody { id: id.get() })
            .send()
            .await?;

        match r {
            r if r.status() == 200 => Ok(()),
            r => Err(ServerErrorResponse::from_response(r).await.into()),
        }
    }

    /// Build the request path for relative `path`.
    ///
    /// # Safety
    ///
    /// Panics in debug builds if `path` contains an absolute path.
    fn url_for(&self, path: &str) -> Url {
        // In non-release builds, assert the path is not an absolute path.
        //
        // Paths should be relative so the full base path is used.
        debug_assert_ne!(
            path.chars().next().unwrap(),
            '/',
            "should not join absolute paths to base URL"
        );
        self.base
            .join(path)
            .expect("failed to construct request URL")
    }

    fn db_url(&self, database: &str) -> Result<Url, ClientError> {
        const DB_PATH: &str = "iox/api/v1/databases/";

        // Perform validation in the client as URL parser silently drops invalid
        // characters
        let name = DatabaseName::new(database).map_err(|_| ClientError::InvalidDatabaseName)?;

        self.url_for(DB_PATH)
            .join(name.as_ref())
            .map_err(|_| ClientError::InvalidDatabaseName)
    }
}

#[cfg(test)]
mod tests {
    use crate::ClientBuilder;
    use rand::{distributions::Alphanumeric, thread_rng, Rng};

    use super::*;

    /// If `TEST_IOX_ENDPOINT` is set, load the value and return it to the
    /// caller.
    ///
    /// If `TEST_IOX_ENDPOINT` is not set, skip the calling test by returning
    /// early. Additionally if `TEST_INTEGRATION` is set, turn this early return
    /// into a panic to force a hard fail for skipped integration tests.
    macro_rules! maybe_skip_integration {
        () => {
            match (
                std::env::var("TEST_IOX_ENDPOINT").is_ok(),
                std::env::var("TEST_INTEGRATION").is_ok(),
            ) {
                (true, _) => std::env::var("TEST_IOX_ENDPOINT").unwrap(),
                (false, true) => {
                    panic!("TEST_INTEGRATION is set which requires running integration tests, but TEST_IOX_ENDPOINT is not")
                }
                _ => {
                    eprintln!("skipping integration test - set TEST_IOX_ENDPOINT to run");
                    return;
                }
            }
        };
    }

    #[tokio::test]
    async fn test_ping() {
        let endpoint = maybe_skip_integration!();
        let c = ClientBuilder::default().build(endpoint).unwrap();
        c.ping().await.expect("ping failed");
    }

    #[tokio::test]
    async fn test_set_writer_id() {
        let endpoint = maybe_skip_integration!();
        let c = ClientBuilder::default().build(endpoint).unwrap();

        c.set_writer_id(NonZeroU32::new(42).unwrap())
            .await
            .expect("set ID failed");
    }

    #[tokio::test]
    async fn test_create_database() {
        let endpoint = maybe_skip_integration!();
        let c = ClientBuilder::default().build(endpoint).unwrap();

        c.set_writer_id(NonZeroU32::new(42).unwrap())
            .await
            .expect("set ID failed");

        c.create_database(rand_name(), &DatabaseRules::default())
            .await
            .expect("create database failed");
    }

    #[tokio::test]
    async fn test_create_database_duplicate_name() {
        let endpoint = maybe_skip_integration!();
        let c = ClientBuilder::default().build(endpoint).unwrap();

        c.set_writer_id(NonZeroU32::new(42).unwrap())
            .await
            .expect("set ID failed");

        let db_name = rand_name();

        c.create_database(db_name.clone(), &DatabaseRules::default())
            .await
            .expect("create database failed");

        let err = c
            .create_database(db_name, &DatabaseRules::default())
            .await
            .expect_err("create database failed");

        assert!(matches!(dbg!(err), CreateDatabaseError::AlreadyExists))
    }

    #[tokio::test]
    async fn test_create_database_invalid_name() {
        let endpoint = maybe_skip_integration!();
        let c = ClientBuilder::default().build(endpoint).unwrap();

        c.set_writer_id(NonZeroU32::new(42).unwrap())
            .await
            .expect("set ID failed");

        let err = c
            .create_database("my_example\ndb", &DatabaseRules::default())
            .await
            .expect_err("expected request to fail");

        assert!(matches!(
            dbg!(err),
            CreateDatabaseError::ClientError(ClientError::InvalidDatabaseName)
        ));
    }

    #[test]
    fn test_default() {
        // Ensures the Default impl does not panic
        let c = Client::default();
        assert_eq!(c.base.as_str(), "http://127.0.0.1:8080/");
    }

    #[test]
    fn test_paths() {
        let c = ClientBuilder::default()
            .build("http://127.0.0.2:8081/proxy")
            .unwrap();

        assert_eq!(
            c.url_for("bananas").as_str(),
            "http://127.0.0.2:8081/proxy/bananas"
        );
    }

    #[test]
    #[should_panic(expected = "absolute paths")]
    fn test_absolute_path_panics() {
        let c = ClientBuilder::default()
            .build("http://127.0.0.2:8081/proxy")
            .unwrap();

        c.url_for("/bananas");
    }

    fn rand_name() -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect()
    }
}
