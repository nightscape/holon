use super::traits::Lens;
use holon_api::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum FieldChange {
    Set(Value),
    Clear,
}

#[derive(Debug, Clone)]
pub struct Updates<T> {
    changes: HashMap<String, FieldChange>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Updates<T> {
    pub fn new() -> Self {
        Self {
            changes: HashMap::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn set<U, L>(&mut self, lens: L, value: U) -> &mut Self
    where
        L: Lens<T, U>,
        U: Into<Value>,
    {
        let field_name = lens.field_name().to_string();
        self.changes
            .insert(field_name, FieldChange::Set(value.into()));
        self
    }

    pub fn clear<U, L>(&mut self, lens: L) -> &mut Self
    where
        L: Lens<T, U>,
    {
        let field_name = lens.field_name().to_string();
        self.changes.insert(field_name, FieldChange::Clear);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.changes.len()
    }

    pub fn get(&self, field_name: &str) -> Option<&FieldChange> {
        self.changes.get(field_name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &FieldChange)> {
        self.changes.iter()
    }
}

impl<T> Default for Updates<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> IntoIterator for Updates<T> {
    type Item = (String, FieldChange);
    type IntoIter = std::collections::hash_map::IntoIter<String, FieldChange>;

    fn into_iter(self) -> Self::IntoIter {
        self.changes.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestStruct {
        name: String,
        age: u32,
        email: Option<String>,
    }

    #[derive(Clone)]
    struct NameLens;
    impl Lens<TestStruct, String> for NameLens {
        fn get(&self, item: &TestStruct) -> Option<String> {
            Some(item.name.clone())
        }

        fn set(&self, item: &mut TestStruct, value: String) {
            item.name = value;
        }

        fn field_name(&self) -> &'static str {
            "name"
        }
    }

    #[derive(Clone)]
    struct AgeLens;
    impl Lens<TestStruct, u32> for AgeLens {
        fn get(&self, item: &TestStruct) -> Option<u32> {
            Some(item.age)
        }

        fn set(&self, item: &mut TestStruct, value: u32) {
            item.age = value;
        }

        fn sql_column(&self) -> &'static str {
            "user_age"
        }

        fn field_name(&self) -> &'static str {
            "age"
        }
    }

    #[derive(Clone)]
    struct EmailLens;
    impl Lens<TestStruct, String> for EmailLens {
        fn get(&self, item: &TestStruct) -> Option<String> {
            item.email.clone()
        }

        fn set(&self, item: &mut TestStruct, value: String) {
            item.email = Some(value);
        }

        fn field_name(&self) -> &'static str {
            "email"
        }
    }

    #[test]
    fn test_updates_new() {
        let updates = Updates::<TestStruct>::new();
        assert!(updates.is_empty());
        assert_eq!(updates.len(), 0);

        assert_eq!(NameLens.sql_column(), "name");
        assert_eq!(NameLens.field_name(), "name");
        assert_eq!(AgeLens.sql_column(), "user_age");
        assert_eq!(AgeLens.field_name(), "age");
        assert_eq!(EmailLens.sql_column(), "email");
        assert_eq!(EmailLens.field_name(), "email");
    }

    #[test]
    fn test_updates_set() {
        let mut updates = Updates::<TestStruct>::new();
        updates.set(NameLens, "John".to_string());

        assert!(!updates.is_empty());
        assert_eq!(updates.len(), 1);
        assert!(matches!(
            updates.get("name"),
            Some(FieldChange::Set(Value::String(s))) if s == "John"
        ));
    }

    #[test]
    fn test_updates_clear() {
        let mut updates = Updates::<TestStruct>::new();
        updates.clear::<String, _>(EmailLens);

        assert!(!updates.is_empty());
        assert_eq!(updates.len(), 1);
        assert!(matches!(updates.get("email"), Some(FieldChange::Clear)));
    }

    #[test]
    fn test_updates_multiple_fields() {
        let mut updates = Updates::<TestStruct>::new();
        updates
            .set(NameLens, "Alice".to_string())
            .set(AgeLens, 30u32)
            .clear::<String, _>(EmailLens);

        assert_eq!(updates.len(), 3);
        assert!(matches!(
            updates.get("name"),
            Some(FieldChange::Set(Value::String(s))) if s == "Alice"
        ));
        assert!(matches!(
            updates.get("age"),
            Some(FieldChange::Set(Value::Integer(30)))
        ));
        assert!(matches!(updates.get("email"), Some(FieldChange::Clear)));
    }

    #[test]
    fn test_updates_overwrite() {
        let mut updates = Updates::<TestStruct>::new();
        updates.set(NameLens, "First".to_string());
        updates.set(NameLens, "Second".to_string());

        assert_eq!(updates.len(), 1);
        assert!(matches!(
            updates.get("name"),
            Some(FieldChange::Set(Value::String(s))) if s == "Second"
        ));
    }

    #[test]
    fn test_updates_iter() {
        let mut updates = Updates::<TestStruct>::new();
        updates.set(NameLens, "Bob".to_string()).set(AgeLens, 25u32);

        let mut count = 0;
        for (field_name, change) in updates.iter() {
            count += 1;
            assert!(field_name == "name" || field_name == "age");
            assert!(matches!(change, FieldChange::Set(_)));
        }
        assert_eq!(count, 2);
    }

    #[test]
    fn test_updates_into_iter() {
        let mut updates = Updates::<TestStruct>::new();
        updates
            .set(NameLens, "Charlie".to_string())
            .set(AgeLens, 35u32);

        let collected: Vec<_> = updates.into_iter().collect();
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn test_field_change_set() {
        let change = FieldChange::Set(Value::String("test".to_string()));
        assert!(matches!(change, FieldChange::Set(_)));
    }

    #[test]
    fn test_field_change_clear() {
        let change = FieldChange::Clear;
        assert!(matches!(change, FieldChange::Clear));
    }
}
