use moonlink::{
    test_append_only_table_profile_on_local_fs, test_no_iceberg_persistence_on_local_fs,
    test_normal_profile_on_local_fs,
};

#[tokio::main]
async fn main() {
    test_normal_profile_on_local_fs().await;
    test_append_only_table_profile_on_local_fs().await;
    test_no_iceberg_persistence_on_local_fs().await;
}
