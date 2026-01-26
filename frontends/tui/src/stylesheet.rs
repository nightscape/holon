use r3bl_tui::{
    new_style, throws_with_return, tui_color, tui_stylesheet, CommonResult, FlexBoxId,
    TuiStylesheet,
};

/// Style IDs for consistent styling across the application
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StyleId {
    // Default/base style
    Default = 0,

    // Block list styles
    BlockSelected = 1,
    BlockNormal = 2,
    BlockCompleted = 3,

    // Component styles
    CheckboxChecked = 4,
    CheckboxUnchecked = 5,
    BadgeDefault = 6,
    BadgeCyan = 7,
    BadgeYellow = 8,

    // Status bar styles
    StatusBarBg = 10,
    StatusBarFg = 11,
    StatusBarSeparator = 12,

    // Title styles
    TitleMain = 20,
}

impl From<StyleId> for u8 {
    fn from(id: StyleId) -> u8 {
        id as u8
    }
}

impl From<StyleId> for FlexBoxId {
    fn from(id: StyleId) -> FlexBoxId {
        FlexBoxId::new(id)
    }
}

/// Create the centralized stylesheet for the application
///
/// This provides consistent theming and makes it easy to modify colors/styles
/// across the entire application from one location.
pub fn create_stylesheet() -> CommonResult<TuiStylesheet> {
    throws_with_return!({
        tui_stylesheet! {
            // Default base style
            new_style!(
                id: {StyleId::Default}
            ),

            // Block list styles
            new_style!(
                id: {StyleId::BlockSelected}
                color_bg: {tui_color!(hex "#333333")}
                color_fg: {tui_color!(hex "#FFFFFF")}
            ),
            new_style!(
                id: {StyleId::BlockNormal}
                color_fg: {tui_color!(hex "#CCCCCC")}
            ),
            new_style!(
                id: {StyleId::BlockCompleted}
                color_fg: {tui_color!(hex "#666666")}
                dim
            ),

            // Checkbox styles
            new_style!(
                id: {StyleId::CheckboxChecked}
                color_fg: {tui_color!(hex "#00FF00")}
                bold
            ),
            new_style!(
                id: {StyleId::CheckboxUnchecked}
                color_fg: {tui_color!(hex "#888888")}
            ),

            // Badge styles
            new_style!(
                id: {StyleId::BadgeDefault}
                color_fg: {tui_color!(hex "#FFFF00")}
                bold
            ),
            new_style!(
                id: {StyleId::BadgeCyan}
                color_fg: {tui_color!(hex "#00FFFF")}
                bold
            ),
            new_style!(
                id: {StyleId::BadgeYellow}
                color_fg: {tui_color!(hex "#FFFF00")}
                bold
            ),

            // Status bar styles
            new_style!(
                id: {StyleId::StatusBarBg}
                color_bg: {tui_color!(hex "#076DEB")}
                color_fg: {tui_color!(hex "#E9C940")}
            ),
            new_style!(
                id: {StyleId::StatusBarFg}
                color_fg: {tui_color!(hex "#E9C940")}
            ),
            new_style!(
                id: {StyleId::StatusBarSeparator}
                color_fg: {tui_color!(hex "#444444")}
                dim
            ),

            // Title styles
            new_style!(
                id: {StyleId::TitleMain}
                color_fg: {tui_color!(hex "#00AAFF")}
                bold
            )
        }
    })
}
