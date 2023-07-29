use std::sync::Arc;

use crate::{
    cell::{mixed::MixedCell, MemoryCellFactory, NaiveCellFactory},
    model,
    platform::Platform,
    scheduler::cell::{Cell, CellFactory},
};

pub struct SimpleMixedCellFactory;

impl CellFactory<MixedCell> for SimpleMixedCellFactory {
    fn new(&self, meta: model::Meta, client: Arc<Platform>) -> Arc<MixedCell> {
        if meta.key.len() >= 30 {
            MixedCell::new(MemoryCellFactory.new(meta, client) as Arc<dyn Cell>)
        } else {
            MixedCell::new(NaiveCellFactory.new(meta, client) as Arc<dyn Cell>)
        }
    }
}
