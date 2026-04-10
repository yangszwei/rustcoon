#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TableId {
    Study,
    Series,
    Instance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Table {
    pub id: TableId,
    pub name: &'static str,
    pub alias: &'static str,
    pub primary_key: &'static str,
}

pub(crate) const STUDIES: Table = Table {
    id: TableId::Study,
    name: "studies",
    alias: "s",
    primary_key: "study_instance_uid",
};

pub(crate) const SERIES: Table = Table {
    id: TableId::Series,
    name: "series",
    alias: "se",
    primary_key: "series_instance_uid",
};

pub(crate) const INSTANCES: Table = Table {
    id: TableId::Instance,
    name: "instances",
    alias: "i",
    primary_key: "sop_instance_uid",
};
