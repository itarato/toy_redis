use crate::commands::Command;
use regex::Regex;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::u128;

pub(crate) const MIN_LAT: f64 = -85.05112878;
pub(crate) const MAX_LAT: f64 = 85.05112878;
pub(crate) const MIN_LON: f64 = -180.0;
pub(crate) const MAX_LON: f64 = 180.0;

pub(crate) type Error = Box<dyn std::error::Error + Send + Sync>;

pub(crate) struct ReaderRole {
    pub(crate) writer_host: String,
    pub(crate) writer_port: u16,
}

#[derive(Hash, PartialEq, Eq)]
pub(crate) enum ClientCapability {
    Psync2,
}

impl ClientCapability {
    pub(crate) fn from_str(raw: &str) -> Option<Self> {
        match raw.to_lowercase().as_str() {
            "psync2" => Some(ClientCapability::Psync2),
            _ => None,
        }
    }
}

#[derive(PartialEq, Eq)]
pub(crate) enum ClientOffsetUpdate {
    UpdateRequested,
    Updating,
    Idle,
}

pub(crate) struct ClientInfo {
    pub(crate) port: Option<u16>,
    pub(crate) capabilities: HashSet<ClientCapability>,
    last_synced_command_index: i64,
    pub(crate) offset: usize,
    pub(crate) offset_update: ClientOffsetUpdate,
}

impl ClientInfo {
    pub(crate) fn new() -> Self {
        Self {
            port: None,
            capabilities: HashSet::new(),
            last_synced_command_index: -1,
            offset: 0,
            offset_update: ClientOffsetUpdate::Idle,
        }
    }
}

pub(crate) struct WriterRole {
    pub(crate) replid: String,
    pub(crate) offset: usize,
    //                          vvv--request-count
    pub(crate) clients: HashMap<u64, ClientInfo>,
    pub(crate) write_queue: VecDeque<Command>,
}

impl WriterRole {
    pub(crate) fn push_write_command(&mut self, command: Command) {
        self.offset += command.into_resp().serialize().len();
        self.write_queue.push_back(command);
    }

    pub(crate) fn pop_write_command(&mut self, request_count: u64) -> Vec<Command> {
        let client_info = self
            .clients
            .get_mut(&request_count)
            .expect("loading client info");

        let last_read_index = client_info.last_synced_command_index;
        let latest_readable_index = self.write_queue.len() as i64 - 1;

        if latest_readable_index == last_read_index {
            return vec![];
        }
        if latest_readable_index < last_read_index {
            panic!("Latest index is greater than last index");
        }

        let mut out = vec![];
        for i in (last_read_index + 1)..=latest_readable_index {
            out.push(self.write_queue[i as usize].clone());
        }

        client_info.last_synced_command_index = latest_readable_index;

        out
    }

    pub(crate) fn update_client_offset(&mut self, request_count: u64, offset: usize) {
        let client_info = self
            .clients
            .get_mut(&request_count)
            .expect("Missing client");
        client_info.offset = offset;
        client_info.offset_update = ClientOffsetUpdate::Idle;
    }

    pub(crate) fn reset_client_offset_state(&mut self, request_count: u64) {
        let client_info = self
            .clients
            .get_mut(&request_count)
            .expect("Missing client");
        client_info.offset_update = ClientOffsetUpdate::Idle;
    }
}

pub(crate) enum ReplicationRole {
    Reader(ReaderRole),
    Writer(WriterRole),
}

impl ReplicationRole {
    pub(crate) fn is_writer(&self) -> bool {
        match self {
            ReplicationRole::Writer(_) => true,
            _ => false,
        }
    }

    pub(crate) fn is_reader(&self) -> bool {
        match self {
            ReplicationRole::Reader(_) => true,
            _ => false,
        }
    }

    pub(crate) fn writer_mut(&mut self) -> &mut WriterRole {
        match self {
            ReplicationRole::Writer(writer) => writer,
            _ => panic!("Caller must ensure role is writer"),
        }
    }

    pub(crate) fn writer(&self) -> &WriterRole {
        match self {
            ReplicationRole::Writer(writer) => writer,
            _ => panic!("Caller must ensure role is writer"),
        }
    }
}

pub(crate) type KeyValuePair = (String, String);

#[derive(Debug, Clone, PartialEq, Eq, Ord)]
pub(crate) struct CompleteStreamEntryID(pub(crate) u128, pub(crate) usize);

#[derive(Debug, Clone)]
pub(crate) enum RangeStreamEntryID {
    Fixed(CompleteStreamEntryID),
    Latest,
}

impl CompleteStreamEntryID {
    pub(crate) fn to_string(&self) -> String {
        format!("{}-{}", self.0, self.1)
    }

    pub(crate) fn max() -> Self {
        CompleteStreamEntryID(u128::MAX, usize::MAX)
    }
}

impl PartialOrd for CompleteStreamEntryID {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.0.cmp(&other.0) {
            std::cmp::Ordering::Greater | std::cmp::Ordering::Less => self.0.partial_cmp(&other.0),
            std::cmp::Ordering::Equal => self.1.partial_cmp(&other.1),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum StreamEntryID {
    Wildcard,
    MsOnly(u128),
    Full(CompleteStreamEntryID),
}

impl StreamEntryID {
    pub(crate) fn to_resp_string(&self) -> String {
        match self {
            StreamEntryID::Wildcard => "*".to_string(),
            StreamEntryID::MsOnly(ms) => ms.to_string(),
            StreamEntryID::Full(id) => format!("{}-{}", id.0, id.1),
        }
    }
}

pub(crate) fn current_time_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX EPOCH")
        .as_millis()
}

pub(crate) fn current_time_secs_f64() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before UNIX EPOCH")
        .as_secs_f64()
}

pub(crate) struct PatternMatcher {
    pattern: Regex,
}

impl PatternMatcher {
    pub(crate) fn new(raw: &str) -> Self {
        let transformed = format!("^{}$", raw.replace('*', ".*").replace('?', "."));
        Self {
            pattern: Regex::new(&transformed).unwrap(),
        }
    }

    pub(crate) fn is_match(&self, other: &str) -> bool {
        self.pattern.is_match(other)
    }
}

#[derive(PartialEq, PartialOrd)]
pub(crate) struct SortedSetElem {
    score: f64,
    member: String,
}

impl Eq for SortedSetElem {}

impl Ord for SortedSetElem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl SortedSetElem {
    pub(crate) fn new(score: f64, member: String) -> Self {
        Self { score, member }
    }
}

pub(crate) struct SortedSetData {
    pub(crate) score: f64,
    lon: f64,
    lat: f64,
}

impl SortedSetData {
    fn with_geo(score: f64, lon: f64, lat: f64) -> Self {
        Self {
            score,
            lon: lon,
            lat: lat,
        }
    }

    fn with_score(score: f64) -> Self {
        let (lon, lat) = decode_geohash(score);

        Self { score, lon, lat }
    }
}

#[derive(Default)]
pub(crate) struct SortedSet {
    ordering: BTreeSet<SortedSetElem>,
    members: HashMap<String, SortedSetData>,
}

impl SortedSet {
    pub(crate) fn remove(&mut self, member: String) -> bool {
        if !self.members.contains_key(&member) {
            return false;
        }

        let elem = SortedSetElem::new(self.members.get(&member).unwrap().score, member.clone());
        self.ordering.remove(&elem);
        self.members.remove(&member);

        true
    }

    pub(crate) fn insert_geo(&mut self, lon: f64, lat: f64, member: String) -> bool {
        let score = encode_geohash(lon, lat);

        let mut is_new = true;
        if self.members.contains_key(&member) {
            self.remove(member.clone());
            is_new = false;
        }

        self.members
            .insert(member.clone(), SortedSetData::with_geo(score, lon, lat));
        self.ordering
            .insert(SortedSetElem::new(score, member.clone()));

        is_new
    }

    pub(crate) fn insert_score(&mut self, score: f64, member: String) -> bool {
        let mut is_new = true;
        if self.members.contains_key(&member) {
            self.remove(member.clone());
            is_new = false;
        }

        self.members
            .insert(member.clone(), SortedSetData::with_score(score));
        self.ordering
            .insert(SortedSetElem::new(score, member.clone()));

        is_new
    }

    pub(crate) fn rank(&self, member: &str) -> Option<usize> {
        self.ordering.iter().position(|elem| elem.member == member)
    }

    pub(crate) fn range(&self, start: usize, end: usize) -> Vec<String> {
        self.ordering
            .iter()
            .skip(start)
            .take(end - start + 1)
            .map(|elem| elem.member.clone())
            .collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.ordering.len()
    }

    pub(crate) fn member_score(&self, member: &str) -> Option<f64> {
        self.members.get(member).map(|elem| elem.score)
    }

    pub(crate) fn member_coords(&self, member: &str) -> Option<(f64, f64)> {
        self.members.get(member).map(|elem| (elem.lon, elem.lat))
    }

    pub(crate) fn members(&self) -> Vec<&String> {
        self.members.keys().collect()
    }
}

fn spread_u32_to_u64(v: u32) -> u64 {
    let mut v = (v & 0xFFFF_FFFF) as u64;

    v = (v | (v << 16)) & 0x0000_FFFF_0000_FFFF;
    v = (v | (v << 8)) & 0x00FF_00FF_00FF_00FF;
    v = (v | (v << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    v = (v | (v << 2)) & 0x3333_3333_3333_3333;
    v = (v | (v << 1)) & 0x5555_5555_5555_5555;

    v
}

pub(crate) fn encode_geohash(lon: f64, lat: f64) -> f64 {
    assert!(lat >= MIN_LAT);
    assert!(lat <= MAX_LAT);
    assert!(lon >= MIN_LON);
    assert!(lon <= MAX_LON);

    let lat_range = MAX_LAT - MIN_LAT;
    let lon_range = MAX_LON - MIN_LON;

    let norm_lat: u32 = ((1u64 << 26u64) as f64 * (lat - MIN_LAT) / lat_range) as u32;
    let norm_lon: u32 = ((1u64 << 26u64) as f64 * (lon - MIN_LON) / lon_range) as u32;

    let lhs64 = spread_u32_to_u64(norm_lat);
    let rhs64 = spread_u32_to_u64(norm_lon);
    let rhs_shifted = rhs64 << 1;

    (lhs64 | rhs_shifted) as f64
}

fn compact_u64_to_u32(mut v: u64) -> u32 {
    v = v & 0x5555555555555555;

    v = (v | (v >> 1)) & 0x3333_3333_3333_3333;
    v = (v | (v >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
    v = (v | (v >> 4)) & 0x00FF_00FF_00FF_00FF;
    v = (v | (v >> 8)) & 0x0000_FFFF_0000_FFFF;
    v = (v | (v >> 16)) & 0x0000_0000_FFFF_FFFF;

    v as u32
}

pub(crate) fn decode_geohash(hash: f64) -> (f64, f64) {
    let rhs = hash as u64 >> 1;
    let lhs = hash as u64;

    let grid_lon = compact_u64_to_u32(rhs) as f64;
    let grid_lat = compact_u64_to_u32(lhs) as f64;

    let lat_range = MAX_LAT - MIN_LAT;
    let lon_range = MAX_LON - MIN_LON;

    let grid_lat_min = MIN_LAT + lat_range * (grid_lat / (1u32 << 26) as f64);
    let grid_lat_max = MIN_LAT + lat_range * ((grid_lat + 1.0) / (1u32 << 26) as f64);
    let grid_lon_min = MIN_LON + lon_range * (grid_lon / (1u32 << 26) as f64);
    let grid_lon_max = MIN_LON + lon_range * ((grid_lon + 1.0) / (1u32 << 26) as f64);

    let lat = (grid_lat_min + grid_lat_max) / 2.0;
    let lon = (grid_lon_min + grid_lon_max) / 2.0;

    (lon, lat)
}

const EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;

pub(crate) fn geohash_get_distance(lon1d: f64, lat1d: f64, lon2d: f64, lat2d: f64) -> f64 {
    let lat1 = lat1d.to_radians();
    let lat2 = lat2d.to_radians();
    let d_lat = lat2 - lat1;
    let d_lon = (lon2d - lon1d).to_radians();

    let a = (d_lat / 2.0).sin().powi(2) + (d_lon / 2.0).sin().powi(2) * lat1.cos() * lat2.cos();
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_IN_METERS * c
}

#[cfg(test)]
mod test {
    use crate::common::{
        decode_geohash, encode_geohash, geohash_get_distance, PatternMatcher, SortedSetElem,
    };

    #[test]
    fn test_pattern_matcher() {
        assert!(PatternMatcher::new("*").is_match("anything"));
        assert!(PatternMatcher::new("*").is_match(""));

        assert!(PatternMatcher::new("*abc").is_match("abc"));
        assert!(PatternMatcher::new("*abc").is_match("cccabc"));

        assert!(!PatternMatcher::new("*abc").is_match("abc "));
        assert!(!PatternMatcher::new("*abc").is_match("ab c"));

        assert!(PatternMatcher::new("*abc*").is_match("abc"));
        assert!(PatternMatcher::new("*abc*").is_match("cccabcddd"));

        assert!(PatternMatcher::new("abc").is_match("abc"));
        assert!(!PatternMatcher::new("abc").is_match(" abc"));
        assert!(!PatternMatcher::new("abc").is_match("abc "));

        assert!(PatternMatcher::new("a?c").is_match("abc"));
        assert!(!PatternMatcher::new("a?c").is_match("ac"));

        assert!(!PatternMatcher::new("a?c").is_match("abbc"));
    }

    #[test]
    fn test_sorted_set_elem_ordering() {
        assert!(SortedSetElem::new(1.23, "Foo".into()) == SortedSetElem::new(1.23, "Foo".into()));
        assert!(SortedSetElem::new(1.22, "Foo".into()) < SortedSetElem::new(1.23, "Foo".into()));
        assert!(SortedSetElem::new(1.24, "Foo".into()) > SortedSetElem::new(1.23, "Foo".into()));
        assert!(SortedSetElem::new(1.23, "Foa".into()) < SortedSetElem::new(1.23, "Foo".into()));
        assert!(SortedSetElem::new(1.23, "Fox".into()) > SortedSetElem::new(1.23, "Foo".into()));
    }

    #[test]
    fn test_geohashing() {
        assert_geohash(100.5252, 13.7220, 3962257306574459.0);
        assert_geohash(116.3972, 39.9075, 4069885364908765.0);
        assert_geohash(13.4105, 52.5244, 3673983964876493.0);
        assert_geohash(12.5655, 55.6759, 3685973395504349.0);
        assert_geohash(77.2167, 28.6667, 3631527070936756.0);
        assert_geohash(85.3206, 27.7017, 3639507404773204.0);
        assert_geohash(-0.1278, 51.5074, 2163557714755072.0);
        assert_geohash(-74.0060, 40.7128, 1791873974549446.0);
        assert_geohash(2.3488, 48.8534, 3663832752681684.0);
        assert_geohash(151.2093, -33.8688, 3252046221964352.0);
        assert_geohash(139.6917, 35.6895, 4171231230197045.0);
        assert_geohash(16.3707, 48.2064, 3673109836391743.0);
    }

    #[test]
    fn test_geohash_decode() {
        dbg!(decode_geohash(3663832614298053.0));
    }

    fn assert_geohash(lon: f64, lat: f64, expexted: f64) {
        let diff = (encode_geohash(lon, lat) - expexted).abs();
        // dbg!(diff);
        assert!(diff < 1.0);
    }

    #[test]
    fn test_geodist1() {
        let actual = geohash_get_distance(
            104.40118753384803,
            -76.97514594029536,
            96.49848464354902,
            44.750533591587164,
        );
        let expected = 13550492.00422531;
        let diff = (actual - expected).abs();
        dbg!(diff);
        assert!(diff < 0.0001);
    }

    #[test]
    fn test_geodist2() {
        let (lon1, lat1) = decode_geohash(encode_geohash(-74.15093763136399, -61.91578641950368));
        let (lon2, lat2) = decode_geohash(encode_geohash(137.26070012474287, 25.91180220379407));

        let actual = geohash_get_distance(lon1, lat1, lon2, lat2);
        let expected = 15385284.571358781;
        let diff = (actual - expected).abs();
        dbg!(actual);
        dbg!(diff);
        assert!(diff < 0.0001);
    }
}
