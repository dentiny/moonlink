/// Possible states:
/// (1) No deletion vector
/// (2) Deletion vector referenced, not requested to delete
/// (3) Deletion vector referenced, requested to delete
/// (4) Deletion vector not referenced and requested to delete
///
/// Difference with data files:
/// - Deletion vector always sits on-disk
/// - Data file has an extra state: not referenced but not requested to deleted
///
/// State transition input:
/// - Persist into iceberg table
/// - Recover from iceberg table
/// - Use deletion vector (including read and compact)
/// - Usage finishes
///
/// State machine transfer:
/// Initial state: no deletion vector
/// - No deletion vector + persist => referenced, not requested to delete
/// - No deletion vector + recover => referenced, not requested to delete
///
/// Initial state: referenced, not requested to delete
/// - Referenced, no delete + use => referenced, no delete
/// - Referenced, no delete + use over => referenced, no delete
///
/// Initial state: referenced, requested to delete
/// - Referenced, to delete + use over & referenced => referenced, to delete
/// - Referenced, to delete + use over & unreferenced => no deletion vector

#[tokio::test]
async fn test_1_persist_2() {}
