use super::clip;

#[test]
fn a_cut_keeps_whole_characters() {
    assert_eq!(clip("ééé", 2), ("éé", true));
    assert_eq!(clip("éé", 2), ("éé", false));
    assert_eq!(clip("é", 0), ("", true));
}
