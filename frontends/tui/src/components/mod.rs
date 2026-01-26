mod block_list;

pub use block_list::BlockListComponent;

use r3bl_tui::FlexBoxId;

/// Component IDs for the application
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComponentId {
    BlockList = 1,
}

impl From<ComponentId> for u8 {
    fn from(id: ComponentId) -> u8 {
        id as u8
    }
}

impl From<ComponentId> for FlexBoxId {
    fn from(id: ComponentId) -> FlexBoxId {
        FlexBoxId::new(id)
    }
}
