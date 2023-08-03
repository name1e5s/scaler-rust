use std::sync::Arc;

use crate::{
    cell::{mixed::MixedCell, MemoryCellFactory, NaiveSet1CellFactory, NaiveSet2CellFactory},
    model,
    platform::Platform,
    scheduler::cell::{Cell, CellFactory},
};

pub struct SimpleMixedCellFactory;

impl CellFactory<MixedCell> for SimpleMixedCellFactory {
    fn new(&self, meta: model::Meta, client: Arc<Platform>) -> Arc<MixedCell> {
        if meta.key.len() >= 30 {
            MixedCell::new(MemoryCellFactory.new(meta, client) as Arc<dyn Cell>)
        } else if meta.key.ends_with("1") {
            MixedCell::new(NaiveSet1CellFactory.new(meta, client) as Arc<dyn Cell>)
        } else {
            MixedCell::new(NaiveSet2CellFactory.new(meta, client) as Arc<dyn Cell>)
        }
    }
}
