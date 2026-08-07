use std::{fmt, ops};

pub mod event;
pub mod log;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Position(u64);

impl Position {
    pub const ZERO: Position = Position(0);

    pub fn new(n: u64) -> Self {
        Position(n)
    }

    pub fn get(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Position {
        Position(self.0 + 1)
    }

    pub fn offset_from(self, base: Position) -> u64 {
        self - base
    }
}

impl From<u64> for Position {
    fn from(n: u64) -> Self {
        Position(n)
    }
}

impl ops::Add<Position> for Position {
    type Output = u64;

    fn add(self, rhs: Position) -> Self::Output {
        self.0.add(rhs.0)
    }
}

impl ops::Add<u64> for Position {
    type Output = u64;

    fn add(self, rhs: u64) -> Self::Output {
        self.0.add(rhs)
    }
}

impl ops::Add<Position> for u64 {
    type Output = u64;

    fn add(self, rhs: Position) -> Self::Output {
        self.add(rhs.0)
    }
}

impl ops::Sub<Position> for Position {
    type Output = u64;

    fn sub(self, rhs: Position) -> Self::Output {
        self.0.sub(rhs.0)
    }
}

impl ops::Sub<u64> for Position {
    type Output = u64;

    fn sub(self, rhs: u64) -> Self::Output {
        self.0.sub(rhs)
    }
}

impl ops::Sub<Position> for u64 {
    type Output = u64;

    fn sub(self, rhs: Position) -> Self::Output {
        self.sub(rhs.0)
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
