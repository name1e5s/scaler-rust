use std::sync::Arc;

use crate::{
    cell::{
        mixed::MixedCell, DirectCellFactory, FreelessCellFactory, MemoryCellFactory,
        NaiveCellFactory,
    },
    model,
    platform::Platform,
    scheduler::cell::{Cell, CellFactory},
};

pub struct SimpleMixedCellFactory;

impl CellFactory<MixedCell> for SimpleMixedCellFactory {
    fn new(&self, meta: model::Meta, client: Arc<Platform>) -> Arc<MixedCell> {
        if meta.key.len() >= 30 {
            MixedCell::new(MemoryCellFactory.new(meta, client) as Arc<dyn Cell>)
        } else if meta.key.starts_with("nodes") {
            MixedCell::new(NaiveCellFactory.new(meta, client) as Arc<dyn Cell>)
        } else if meta.key.starts_with("csinodes") {
            MixedCell::new(DirectCellFactory.new(meta, client) as Arc<dyn Cell>)
        } else {
            MixedCell::new(FreelessCellFactory.new(meta, client) as Arc<dyn Cell>)
        }
    }
}
