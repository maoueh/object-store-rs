use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, path::Path, ObjectStore,
};
use url::Url;

pub struct Store {
    store: Arc<dyn ObjectStore>,
    base: String,
}

pub fn new<S: AsRef<str>>(store_url: S) -> Result<Store, anyhow::Error> {
    let store_url = store_url.as_ref();
    let url = match Url::parse(store_url) {
        Ok(url) => url,
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            let absolute_path = std::fs::canonicalize(store_url)
                .map_err(|e| anyhow::anyhow!("Invalid store URL: {}: {}", store_url, e))?;

            Url::parse(&format!("file://{}", absolute_path.to_string_lossy()))
                .with_context(|| format!("Invalid store URL: {}", store_url))?
        }
        Err(e) => Err(e).with_context(|| format!("Invalid store URL: {}", store_url))?,
    };

    match url.scheme() {
        "s3" => {
            unimplemented!("s3://... support not implemented yet")
        }
        "gs" => {
            let bucket = url.host_str().ok_or_else(|| anyhow::anyhow!("No bucket"))?;
            let path = url.path();

            let store = GoogleCloudStorageBuilder::new()
                .with_bucket_name(bucket.to_string())
                .build()?;

            Ok(Store {
                store: Arc::new(store),
                base: match path.starts_with("/") {
                    false => path.to_string(),
                    true => path[1..].to_string(),
                },
            })
        }
        "file" => {
            let store = LocalFileSystem::new_with_prefix(url.path()).context("new local store")?;

            Ok(Store {
                store: Arc::new(store),
                base: "".to_string(),
            })
        }
        _ => Err(anyhow::anyhow!("Unsupported scheme: {}", url.scheme()))?,
    }
}

impl Store {
    pub async fn object_reader(
        &self,
        path: &String,
    ) -> Result<BoxStream<'static, Result<Bytes, object_store::Error>>, anyhow::Error> {
        let content = self.store.get(&self.join_path(path)).await?;

        Ok(content.into_stream())
    }

    fn join_path(&self, path: &String) -> Path {
        Path::from(format!("{}/{}", self.base, path.trim_start_matches('/')))
    }
}
