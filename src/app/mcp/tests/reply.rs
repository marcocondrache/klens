use super::*;

#[test]
fn a_result_of_exactly_the_budget_fits() {
    let padded = |bytes| CallToolResult::success(vec![ContentBlock::text("x".repeat(bytes))]);
    let overhead = serde_json::to_vec(&padded(0)).expect("json").len();

    assert!(fits(&padded(RESULT_BYTES - overhead)));
    assert!(!fits(&padded(RESULT_BYTES - overhead + 1)));
}

#[test]
fn a_list_returns_25_rows_unless_asked_and_between_1_and_100() {
    let kept = |asked| {
        let page = Page::new("rows", vec![0; 200], asked, None);
        json!(page)["rows"].as_array().expect("rows").len()
    };
    assert_eq!(kept(None), 25);
    assert_eq!(kept(Some(0)), 1);
    assert_eq!(kept(Some(3)), 3);
    assert_eq!(kept(Some(100)), 100);
    assert_eq!(kept(Some(101)), 100);
}

#[test]
fn a_list_says_whether_its_limit_or_the_result_size_cut_it() {
    let showing = |rows: usize, asked: usize, narrow: Option<&'static str>| {
        let rows = vec!["x".repeat(1000); rows];
        #[derive(serde::Serialize)]
        struct Probe {
            #[serde(flatten)]
            page: Page<String>,
        }
        reply!(Probe: page);
        let result = fit(Probe {
            page: Page::new("rows", rows, Some(asked), narrow),
        });
        let result = serde_json::to_value(result).expect("json");
        result["structuredContent"]["showing"]
            .as_str()
            .expect("showing")
            .to_owned()
    };
    let narrow = Some("pass `nameContains`");

    assert_eq!(showing(5, 25, narrow), "5 of 5");
    assert_eq!(
        showing(5, 3, narrow),
        "3 of 5; pass `nameContains`, or raise `limit`, to see others"
    );
    assert_eq!(showing(5, 3, None), "3 of 5; raise `limit` to see others");
    for (narrow, rest) in [
        (
            narrow,
            "50, as no more fit the result; pass `nameContains` to see others",
        ),
        (None, "50, as no more fit the result"),
    ] {
        let cut = showing(50, 100, narrow);
        let (shown, after) = cut.split_once(" of ").expect("a count");
        assert!(
            shown.parse::<usize>().is_ok_and(|shown| shown < 50),
            "{cut}"
        );
        assert_eq!(after, rest);
    }
}
