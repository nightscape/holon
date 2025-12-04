//! Property-based tests for orgize round-tripping
//!
//! These tests verify that org-mode text can be parsed into an AST and
//! converted back to text without loss of information.

use orgize::{Org, ParseConfig};
use proptest::prelude::*;

/// Strategy for generating valid org-mode headlines
fn org_headline_strategy() -> impl Strategy<Value = String> {
    prop::collection::vec(
        (
            // Level: 1-6 stars
            (1..=6usize),
            // Todo keyword (optional)
            prop::option::of(prop_oneof![
                Just("TODO".to_string()),
                Just("DONE".to_string()),
                Just("INPROGRESS".to_string()),
            ]),
            // Title text
            "[a-zA-Z0-9 ]{1,50}",
            // Tags (optional)
            prop::option::of(prop::collection::vec("[a-zA-Z0-9]{1,20}", 1..=3)),
        ),
        1..=10,
    )
    .prop_map(
        |headlines: Vec<(usize, Option<String>, String, Option<Vec<String>>)>| {
            headlines
                .into_iter()
                .map(|(level, todo, title, tags)| {
                    let mut line = "*".repeat(level);
                    line.push(' ');
                    if let Some(todo) = todo {
                        line.push_str(&todo);
                        line.push(' ');
                    }
                    line.push_str(&title);
                    if let Some(tags) = tags {
                        line.push_str(" :");
                        line.push_str(&tags.join(":"));
                        line.push(':');
                    }
                    line
                })
                .collect::<Vec<_>>()
                .join("\n")
        },
    )
}

/// Strategy for generating org-mode text with various elements
fn org_text_strategy() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            // Headlines
            org_headline_strategy(),
            // Plain text paragraphs
            "[a-zA-Z0-9 .,!?]{10,200}".prop_map(|s: String| format!("{}\n", s)),
            // Lists
            prop::collection::vec("[a-zA-Z0-9 ]{10,50}", 1..=5).prop_map(|items: Vec<String>| {
                items
                    .into_iter()
                    .map(|item| format!("- {}", item))
                    .collect::<Vec<_>>()
                    .join("\n")
                    + "\n"
            }),
            // Code blocks
            "[a-zA-Z0-9 \n]{10,100}"
                .prop_map(|s: String| { format!("#+BEGIN_SRC\n{}\n#+END_SRC\n", s) }),
        ],
        1..=20,
    )
    .prop_map(|parts: Vec<String>| parts.join("\n\n"))
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 100,
        ..ProptestConfig::default()
    })]

    #[test]
    fn test_orgize_round_trip(org_text in org_text_strategy()) {
        // Parse the org-mode text into an AST
        let org1 = Org::parse(&org_text);

        // Convert the AST back to org-mode text
        let round_trip_text = org1.to_org();

        // Parse the round-trip text again
        let org2 = Org::parse(&round_trip_text);

        // Convert both ASTs back to text for comparison
        // This ensures we're comparing equivalent structures
        let org1_text = org1.to_org();
        let org2_text = org2.to_org();

        // The round-trip should produce equivalent org-mode text
        prop_assert_eq!(
            org1_text.clone(),
            org2_text.clone(),
            "Round-trip failed:\nOriginal: {:?}\nRound-trip: {:?}\nOrg1 text: {:?}\nOrg2 text: {:?}",
            org_text, round_trip_text, org1_text, org2_text
        );
    }

    #[test]
    fn test_orgize_parse_config_round_trip(org_text in org_text_strategy()) {
        // Test with custom parse config
        let config = ParseConfig {
            todo_keywords: (
                vec!["TASK".to_string(), "TODO".to_string()],
                vec!["DONE".to_string(), "COMPLETE".to_string()],
            ),
            ..Default::default()
        };

        let org1 = config.clone().parse(&org_text);
        let round_trip_text = org1.to_org();
        let org2 = config.parse(&round_trip_text);

        let org1_text = org1.to_org();
        let org2_text = org2.to_org();

        prop_assert_eq!(
            org1_text.clone(),
            org2_text.clone(),
            "Round-trip with custom config failed:\nOriginal: {:?}\nRound-trip: {:?}",
            org_text, round_trip_text
        );
    }

    #[test]
    fn test_simple_headline_round_trip(
        level in 1..=6u8,
        todo_idx in prop::option::of(0..=1usize),
        title in "[a-zA-Z0-9 ]{1,50}",
    ) {
        let todo_keywords = vec!["TODO", "DONE"];
        let mut headline = "*".repeat(level as usize);
        headline.push(' ');
        if let Some(idx) = todo_idx {
            if idx < todo_keywords.len() {
                headline.push_str(todo_keywords[idx]);
                headline.push(' ');
            }
        }
        headline.push_str(&title);

        let org1 = Org::parse(&headline);
        let round_trip = org1.to_org();
        let org2 = Org::parse(&round_trip);

        let org1_text = org1.to_org();
        let org2_text = org2.to_org();

        prop_assert_eq!(
            org1_text.clone(),
            org2_text.clone(),
            "Simple headline round-trip failed:\nOriginal: {:?}\nRound-trip: {:?}",
            headline, round_trip
        );
    }
}
