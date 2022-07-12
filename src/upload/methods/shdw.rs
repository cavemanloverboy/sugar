use std::{fs, ops::Deref, sync::Arc};

use anchor_lang::Key;
use async_trait::async_trait;
use data_encoding::HEXLOWER;
use reqwest::{
    multipart::{Form, Part},
    StatusCode, Error,
};
use ring::digest::{Context, SHA256};
use solana_program::pubkey;
use tokio::task::JoinHandle;
use std::io::stdin;
use std::collections::HashMap;
use std::convert::TryInto;

use crate::{
    common::*,
    config::*,
    upload::{
        assets::{get_updated_metadata, AssetPair, DataType},
        uploader::{AssetInfo, ParallelUploader, Prepare, MOCK_URI_SIZE},
        UploadError,
    },
    utils::*,
};

// Shadow Drive program id.
const SHADOW_DRIVE_PROGRAM_ID: Pubkey = pubkey!("2e1wdyNhUvE76y6yUCvah2KaviavMJYKoRun8acMRBZZ");
// Shadow Drive mainnet endpoint.
const MAINNET_ENDPOINT: &str = "https://shadow-storage.genesysgo.net";
// Shadow Drive devnet endpoint.
const DEVNET_ENDPOINT: &str = "https://shadow-storage-dev.genesysgo.net";
// Shadow Drive files location.
const SHDW_DRIVE_LOCATION: &str = "https://shdw-drive.genesysgo.net";

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
pub struct StorageInfo {
    pub reserved_bytes: u64,
    pub current_usage: u64,
    pub immutable: bool,
    pub owner1: Option<String>,
    pub owner2: Option<String>,
}

pub struct Config {
    endpoint: String,
    keypair: Keypair,
    storage_account: Pubkey,
    storage_info: StorageInfo,
}

pub struct SHDWMethod(Arc<Config>);

impl Deref for SHDWMethod {
    type Target = Arc<Config>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SHDWMethod {
    pub async fn new(sugar_config: &SugarConfig, config_data: &ConfigData) -> Result<Self> {


        // Set up rpc client and set shadow drive end point
        let client = setup_client(sugar_config)?;
        let program = client.program(SHADOW_DRIVE_PROGRAM_ID);
        let solana_cluster: Cluster = get_cluster(program.rpc())?;
        let endpoint = match solana_cluster {
            Cluster::Devnet => DEVNET_ENDPOINT,
            Cluster::Mainnet => MAINNET_ENDPOINT,
        };

        // Gather user keypair
        let key_bytes = sugar_config.keypair.to_bytes();
        let keypair = Keypair::from_bytes(&key_bytes)?;

        // If user provides a storage account public key,
        // send http request to get storage account info
        if let Some(pubkey) = &config_data.shdw_storage_account {

            // Set up http client
            let http_client = reqwest::Client::new();

            // Construct request
            let mut json = HashMap::new();
            json.insert("storage_account", pubkey);

            // Send POST request to shadow drive for storage account info
            let response = http_client
                .post(format!("{endpoint}/storage-account-info"))
                .json(&json)
                .send()
                .await?;

            match response.status() {

                // Handle successful request by unpacking response, constructing return value
                StatusCode::OK => {

                    let body = response.json::<Value>().await?;
                    let storage_info: StorageInfo = serde_json::from_value(body)?;

                    return Ok(Self(Arc::new(Config {
                        endpoint: endpoint.to_string(),
                        keypair,
                        storage_account: Pubkey::from_str(pubkey)?,
                        storage_info,
                    })))
                },

                // If request was not sucessful, return error
                code => return Err(anyhow!("Failed to fetch storage account {pubkey} info: {code}"))
            }
        } else {

            // If the user did not provide a storage account public key,
            // attempt to initialize the smallest possible storage account

            // First ask user for permission to initialize a storage account
            get_user_consent_to_initialize_storage_account()?;

            // Then initialize the storage account
            let (storage_account, storage_info) = initialize_storage_account(&keypair)?;

            // Return this new storage account
            Ok(Self(Arc::new(Config {
                endpoint: endpoint.to_string(),
                keypair,
                storage_account,
                storage_info,
            })))
        }
    }
}

#[async_trait]
impl Prepare for SHDWMethod {
    async fn prepare(
        &self,
        sugar_config: &SugarConfig,
        assets: &HashMap<isize, AssetPair>,
        asset_indices: Vec<(DataType, &[isize])>,
    ) -> Result<()> {

        // Old metaplex stuff
        {
            // // Calculates the size of the files to upload. This assumes that the total
            // // storage has enough space to hold the collection as assets might already
            // // exist and therefore will be replaced
            // let mut total_size = 0;
        }

        // Get existing files and their sizes
        let existing_files: HashMap<String, u64> = get_file_sizes(self.0.as_ref()).await?;

        // Find out how many additional bytes are required, if any
        // This needs to be a signed integer because if new assets are 
        // smaller we may not require additional space.
        let mut additional_required_bytes: i64 = 0;

        for (data_type, indices) in asset_indices {
            match data_type {
                DataType::Image => {
                    for index in indices {

                        // Get item size
                        let item = assets.get(index).unwrap();
                        let path = Path::new(&item.image);
                        let item_size = std::fs::metadata(path)?.len();

                        // Check if this file exists in storage account
                        let get_current_size: Option<u64> = existing_files
                            .get(&item.image)
                            .map(|x| *x);

                        // Add file size and subtract existing size, if necessary
                        additional_required_bytes += item_size.try_into().unwrap();
                        if let Some(current_size) = get_current_size {
                            additional_required_bytes -= current_size.try_into().unwrap();
                        }
                    }
                }
                DataType::Animation => {
                    for index in indices {
                        let item = assets.get(index).unwrap();

                        if let Some(animation) = &item.animation {
                            
                            // Add file size
                            let path = Path::new(animation);
                            let item_size = std::fs::metadata(path)?.len();
                            additional_required_bytes += item_size.try_into().unwrap();

                            // Subtract existing file size if necessary
                            
                        }


                    }
                }
                DataType::Metadata => {
                    let mock_uri = "x".repeat(MOCK_URI_SIZE);

                    for index in indices {
                        let item = assets.get(index).unwrap();
                        let animation = if item.animation.is_some() {
                            Some(mock_uri.clone())
                        } else {
                            None
                        };

                        total_size +=
                            get_updated_metadata(&item.metadata, &mock_uri.clone(), &animation)?
                                .into_bytes()
                                .len() as u64;
                    }
                }
            }
        }

        // Check if user have enough storage available on this storage account
        let storage_available: u64 = self.storage_info.reserved_bytes
            .checked_sub(self.storage_info.current_usage).unwrap();

        if storage_available < total_size {

            get_user_consent_to_expand_storage_account(total_size - storage_available)?;
            expand_storage_account(sugar_config)
        }

        Ok(())
    }
}

#[async_trait]
impl ParallelUploader for SHDWMethod {
    fn upload_asset(&self, asset_info: AssetInfo) -> JoinHandle<Result<(String, String)>> {
        let config = self.0.clone();
        tokio::spawn(async move { config.send(asset_info).await })
    }
}

impl Config {
    async fn send(&self, asset_info: AssetInfo) -> Result<(String, String)> {
        let data = match asset_info.data_type {
            DataType::Image => fs::read(&asset_info.content)?,
            DataType::Metadata => asset_info.content.into_bytes(),
            DataType::Animation => fs::read(&asset_info.content)?,
        };

        let mut context = Context::new(&SHA256);
        context.update(asset_info.name.as_bytes());
        let hash = HEXLOWER.encode(context.finish().as_ref());

        let message = format!(
            "Shadow Drive Signed Message:\n\
            Storage Account: {}\n\
            Upload files with hash: {hash}",
            self.storage_account
        );

        let signature = self.keypair.sign_message(message.as_bytes()).to_string();

        let mut form = Form::new();
        let file = Part::bytes(data)
            .file_name(asset_info.name.clone())
            .mime_str(asset_info.content_type.as_str())?;
        form = form
            .part("file", file)
            .text("message", signature)
            .text("overwrite", "true")
            .text("signer", self.keypair.pubkey().to_string())
            .text("storage_account", self.storage_account.to_string())
            .text("fileNames", asset_info.name.to_string());

        let http_client = reqwest::Client::new();
        let response = http_client
            .post(format!("{}/upload", self.endpoint))
            .multipart(form)
            .send()
            .await?;
        let status = response.status();

        if status.is_success() {
            Ok((
                asset_info.asset_id,
                format!(
                    "{SHDW_DRIVE_LOCATION}/{}/{}",
                    self.storage_account, asset_info.name
                ),
            ))
        } else {
            Err(anyhow!(UploadError::SendDataFailed(format!(
                "Error uploading file ({}): {}",
                status,
                response.text().await?,
            ))))
        }
    }
}



fn get_user_consent_to_initialize_storage_account() -> Result<()> {

    println!("\nNo Shadow Drive storage account was provided.\n\
                Do you wish to create one? [y/n] (abort otherwise)");

    // Get user input
    let mut user_input = String::with_capacity(3);
    stdin().read_line(&mut user_input).unwrap();

    if ["yes", "y"].iter().any(|x| *x == user_input.trim().to_lowercase()) {
        return Ok(())
    } else {
        return Err(UploadError::UserRejectedSHDWStorageAccountInit.into())
    }
}


/// This function is to be called within `SHDWMethod::new` when a user did not specify
/// a storage account pubkey. It initializes a storage account with the minimum allowed
/// storage
fn initialize_storage_account(keypair: &Keypair) -> Result<(Pubkey, StorageInfo)> {

    todo!()
}



fn get_user_consent_to_expand_storage_account(addtl_bytes_required: u64) -> Result<()> {

    println!("\nNot enough storage in your account to store all assets.\n\
                Do you wish to expand the storage account by {addtl_bytes_required} bytes)\n\
                to fit all assets? [y/n] (abort otherwise)");

    // Get user input
    let mut user_input = String::with_capacity(3);
    stdin().read_line(&mut user_input).unwrap();

    if ["yes", "y"].iter().any(|x| *x == user_input.trim().to_lowercase()) {
        return Ok(())
    } else {
        return Err(UploadError::UserRejectedSHDWStorageAccountInit.into())
    }
}


fn expand_storage_account(sugar_config: &SugarConfig, additional_bytes_required: u64) -> Result<()> {


    todo!();

    Ok(())
}



/// This function fetches all of the files and their sizes
/// within a storage account 
async fn get_file_sizes(config: &Config) -> Result<HashMap<String, u64>> {

    // Set up http client
    let http_client = reqwest::Client::new();

    // Construct request
    let mut json = HashMap::new();
    json.insert("storage_account", config.storage_account);

    // Send POST request to shadow drive for storage account info
    let response = http_client
        .post(format!("{}/list-objects-and-sizes",config.endpoint))
        .json(&json)
        .send()
        .await?;


    match response.status() {

        // Handle successful request by unpacking response, constructing return value
        StatusCode::OK => {

            // Initialize hashmap
            let mut hashmap = HashMap::new();

            // Get files and their sizes
            let body = response.json::<Value>().await?;
            let files: Files = serde_json::from_value(body)?;

            // Construct hashmap
            for file in files.files {
                hashmap.insert(file.file_name, file.size);
            }

            return Ok(hashmap)
        },

        // If request was not sucessful, return error
        code => return Err(anyhow!("Failed to get file sizes for {} info: {code}", config.storage_account))
    }
}


#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct Files {
    files: Vec<File>
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct File {
    file_name: String,
    size: u64,
    last_modified: String,
}