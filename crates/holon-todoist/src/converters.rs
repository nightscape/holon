use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    Normal,
    Medium,
    High,
    Highest,
}

pub const fn int_to_priority(p: i32) -> Priority {
    match p {
        2 => Priority::Medium,
        3 => Priority::High,
        4 => Priority::Highest,
        _ => Priority::Normal,
    }
}

pub const fn priority_to_int(p: &Priority) -> i32 {
    match p {
        Priority::Medium => 2,
        Priority::High => 3,
        Priority::Highest => 4,
        Priority::Normal => 1,
    }
}

pub fn str_to_date(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = d.and_hms_opt(0, 0, 0)?;
        return Some(DateTime::from_naive_utc_and_offset(dt, Utc));
    }

    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(DateTime::from_naive_utc_and_offset(dt, Utc));
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.to_utc());
    }

    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DueString {
    Today,
    Tomorrow,
    ThisWeekend,
    NextWeek,
    NoDate,
}

impl DueString {
    pub fn as_str(&self) -> &'static str {
        match self {
            DueString::Today => "today",
            DueString::Tomorrow => "tomorrow",
            DueString::ThisWeekend => "weekend",
            DueString::NextWeek => "next week",
            DueString::NoDate => "no date",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_conversion() {
        assert_eq!(int_to_priority(1), Priority::Normal);
        assert_eq!(int_to_priority(2), Priority::Medium);
        assert_eq!(int_to_priority(3), Priority::High);
        assert_eq!(int_to_priority(4), Priority::Highest);
        assert_eq!(int_to_priority(0), Priority::Normal);
        assert_eq!(int_to_priority(99), Priority::Normal);
    }

    #[test]
    fn test_priority_to_int() {
        assert_eq!(priority_to_int(&Priority::Normal), 1);
        assert_eq!(priority_to_int(&Priority::Medium), 2);
        assert_eq!(priority_to_int(&Priority::High), 3);
        assert_eq!(priority_to_int(&Priority::Highest), 4);
    }

    #[test]
    fn test_str_to_date() {
        assert!(str_to_date("2025-01-15").is_some());

        assert!(str_to_date("2025-01-15T14:30:00.000").is_some());

        assert!(str_to_date("2025-01-15T14:30:00Z").is_some());

        assert!(str_to_date("invalid").is_none());
    }

    #[test]
    fn test_due_string() {
        assert_eq!(DueString::Today.as_str(), "today");
        assert_eq!(DueString::Tomorrow.as_str(), "tomorrow");
        assert_eq!(DueString::ThisWeekend.as_str(), "weekend");
        assert_eq!(DueString::NextWeek.as_str(), "next week");
        assert_eq!(DueString::NoDate.as_str(), "no date");
    }
}
