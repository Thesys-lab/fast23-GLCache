// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! Possible errors returned by segment operations.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SegmentsError {
    #[error("bad segment id")]
    BadSegmentId,
    // #[error("item relink failure")]
    // RelinkFailure,
    #[error("evictable segment chain too short")]
    EvictableSegmentChainTooShort,
    #[error("no evictable segments")]
    NoEvictableSegments,
    #[error("evict failure")]
    EvictFailure,
}

#[derive(Error, Debug)]
pub enum NotEvictableReason {
    // #[error("too young")]
    // YoungAge, 
    #[error("close to expire")]
    CloseToExpire, 
    #[error("no next seg")]
    NoNextSeg, 
    #[error("evictable flag is set")]
    NotEvictableFlag,
    #[error("this round not evictable")]
    NotEvictableThisRound,
    #[error("yes")]
    CanEvict,
}

// pub enum NotEvictableReason<'a> {
//     YoungAge(&'a str), 
//     CloseToExpire(&'a str), 
//     NoNextSeg(&'a str), 
// }
