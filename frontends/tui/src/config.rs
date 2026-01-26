use anyhow::Result;
use r3bl_tui::{Key, KeyState, ModifierKeysMask, SpecialKey};
use serde::Deserialize;
use std::fs;
use std::path::Path;

/// Configuration for key bindings
#[derive(Debug, Clone, Deserialize)]
pub struct KeyBindingConfig {
    pub bindings: Vec<KeyBinding>,
}

/// A single key binding entry
#[derive(Debug, Clone, Deserialize)]
pub struct KeyBinding {
    pub key: KeySpec,
    #[serde(default)]
    pub modifiers: Vec<ModifierSpec>,
    pub context: BindingContext,
    pub action: Action,
}

/// Specification for a key (character or special key)
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum KeySpec {
    Character(String), // Single character like "x"
    Special(String),   // Special key like "Up", "Down", "Tab", etc.
}

/// Modifier key specification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModifierSpec {
    Ctrl,
    Shift,
    Alt,
}

/// Context in which the binding applies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BindingContext {
    Navigation,
    Editing,
}

/// Action to execute when binding is triggered
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Action {
    Operation(String), // Operation name like "indent", "outdent"
    Special(String),   // Special action like "toggle_completion", "start_editing", "save_and_exit"
}

impl KeyBindingConfig {
    /// Load key bindings from a YAML file
    pub fn load_from_file(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path).map_err(|e| {
            anyhow::anyhow!("Failed to read keybindings file {}: {}", path.display(), e)
        })?;

        let config: KeyBindingConfig = serde_yaml::from_str(&content).map_err(|e| {
            anyhow::anyhow!("Failed to parse keybindings YAML {}: {}", path.display(), e)
        })?;

        Ok(config)
    }

    /// Find a binding that matches the given key, modifiers, and context
    pub fn find_binding(
        &self,
        key: &Key,
        modifiers: &ModifierKeysMask,
        context: BindingContext,
    ) -> Option<&Action> {
        for binding in &self.bindings {
            // Check context match
            if binding.context != context {
                continue;
            }

            // Check key match
            let key_matches = match (&binding.key, key) {
                (KeySpec::Character(ref spec_char), Key::Character(actual_char)) => {
                    // Handle single character strings
                    spec_char
                        .chars()
                        .next()
                        .map(|c| c == *actual_char)
                        .unwrap_or(false)
                }
                (KeySpec::Special(ref spec_name), Key::SpecialKey(actual_special)) => {
                    Self::special_key_matches(spec_name, actual_special)
                }
                _ => false,
            };

            if !key_matches {
                continue;
            }

            // Check modifiers match
            let modifiers_match = binding.modifiers.iter().all(|mod_spec| match mod_spec {
                ModifierSpec::Ctrl => modifiers.ctrl_key_state == KeyState::Pressed,
                ModifierSpec::Shift => modifiers.shift_key_state == KeyState::Pressed,
                ModifierSpec::Alt => modifiers.alt_key_state == KeyState::Pressed,
            }) && {
                // Ensure no extra modifiers are pressed that aren't in the binding
                let required_ctrl = binding.modifiers.contains(&ModifierSpec::Ctrl);
                let required_shift = binding.modifiers.contains(&ModifierSpec::Shift);
                let required_alt = binding.modifiers.contains(&ModifierSpec::Alt);

                (modifiers.ctrl_key_state == KeyState::Pressed) == required_ctrl
                    && (modifiers.shift_key_state == KeyState::Pressed) == required_shift
                    && (modifiers.alt_key_state == KeyState::Pressed) == required_alt
            };

            if modifiers_match {
                return Some(&binding.action);
            }
        }

        None
    }

    /// Check if a special key name matches the actual SpecialKey enum
    fn special_key_matches(spec_name: &str, actual: &SpecialKey) -> bool {
        match spec_name {
            "Up" => matches!(actual, SpecialKey::Up),
            "Down" => matches!(actual, SpecialKey::Down),
            "Left" => matches!(actual, SpecialKey::Left),
            "Right" => matches!(actual, SpecialKey::Right),
            "Enter" => matches!(actual, SpecialKey::Enter),
            "Tab" => matches!(actual, SpecialKey::Tab),
            "Esc" | "Escape" => matches!(actual, SpecialKey::Esc),
            "Backspace" => matches!(actual, SpecialKey::Backspace),
            "Delete" => matches!(actual, SpecialKey::Delete),
            "Home" => matches!(actual, SpecialKey::Home),
            "End" => matches!(actual, SpecialKey::End),
            "PageUp" => matches!(actual, SpecialKey::PageUp),
            "PageDown" => matches!(actual, SpecialKey::PageDown),
            _ => false,
        }
    }

    /// Create an empty default configuration
    pub fn empty() -> Self {
        Self {
            bindings: Vec::new(),
        }
    }
}
