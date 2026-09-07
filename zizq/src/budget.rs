// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the MIT License. See LICENSE file for details.

//! Server-side throttling: rate limits and concurrency limits.
//!
//! A budget is a named pool of tokens managed under a
//! [`BudgetStrategy`]. Jobs bind to one or more budgets, each with a
//! cost, and a job must debit that cost from every budget it is bound
//! to before the server will dispatch it.
//!
//! This happens on the server, before a worker ever sees the job.
//! Workers stay naive: no waiting, no sleeping, and no re-queueing
//! something that arrived too early.
//!
//! Budgets require a Zizq Pro licence on the server.

use std::borrow::Cow;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::job_patch::Field;

/// How a budget's tokens are managed.
///
/// The two strategies answer different questions. `WhileInFlight` caps
/// how much runs *at once*; `TimeBased` caps how often work *starts*
/// and says nothing about overlap. A job may bind to both.
///
/// ```
/// use std::time::Duration;
/// use zizq::BudgetStrategy;
///
/// // At most 20 of these run concurrently.
/// let concurrency = BudgetStrategy::WhileInFlight;
///
/// // At most 10,000 dispatches an hour.
/// let rate = BudgetStrategy::time_based(Duration::from_secs(3600));
///
/// // The same, but never more than 500 dispatched at once.
/// let paced = BudgetStrategy::TimeBased {
///     duration: Duration::from_secs(3600),
///     burst: Some(500),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "StrategyWire", into = "StrategyWire")]
pub enum BudgetStrategy {
    /// A rate limit: the full allocation replenishes over `duration`,
    /// as a continuous drip rather than in fixed windows.
    TimeBased {
        /// Period over which the full allocation replenishes.
        duration: Duration,

        /// Most tokens the pool may hold at once.
        ///
        /// `None` means the allocation, which is standard token-bucket
        /// behaviour: an idle pool is full, so the first work to
        /// arrive gets a whole allocation at once and only then
        /// settles to the drip. Set this lower to cap that spike —
        /// `Some(1)` paces dispatches evenly with no overshoot at all.
        burst: Option<u32>,
    },

    /// A concurrency limit: tokens are held for as long as the job
    /// runs and released when it stops, on success or on failure.
    /// There is no clock involved.
    WhileInFlight,

    /// A strategy this client version doesn't recognise — e.g. a newer
    /// server introduced one the client predates.
    ///
    /// The catch-all keeps an unknown strategy from failing the whole
    /// decode and cascading to a failed [`Budget`] or `list_budgets`
    /// page, so older clients keep working against newer servers. The
    /// raw type name is preserved, so writing one back is refused by
    /// the server rather than silently turned into something else.
    Unknown {
        /// The strategy name as the server reported it.
        kind: String,
    },
}

impl BudgetStrategy {
    /// A rate limit over `duration`, with no burst ceiling.
    ///
    /// `const`, so it can be used in a [`JobKind::BUDGETS`] entry.
    ///
    /// [`JobKind::BUDGETS`]: crate::JobKind::BUDGETS
    pub const fn time_based(duration: Duration) -> Self {
        BudgetStrategy::TimeBased {
            duration,
            burst: None,
        }
    }

    /// The strategy name as it appears on the wire.
    pub fn kind(&self) -> &str {
        match self {
            BudgetStrategy::TimeBased { .. } => "time_based",
            BudgetStrategy::WhileInFlight => "while_in_flight",
            BudgetStrategy::Unknown { kind } => kind,
        }
    }
}

/// Wire form of a strategy.
///
/// Deliberately not a serde tagged enum: serde ignores surplus fields on
/// a unit variant of an internally-tagged enum, which would silently accept
/// a `duration_ms` on a `while_in_flight` budget.
#[derive(Serialize, Deserialize)]
struct StrategyWire {
    #[serde(rename = "type")]
    kind: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    duration_ms: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    burst: Option<u32>,
}

impl From<StrategyWire> for BudgetStrategy {
    fn from(wire: StrategyWire) -> Self {
        match wire.kind.as_str() {
            // A `time_based` budget always has a period. Treating a
            // missing one as unknown rather than defaulting it keeps
            // the client from inventing a rate the server never set.
            "time_based" => match wire.duration_ms {
                Some(ms) => BudgetStrategy::TimeBased {
                    duration: Duration::from_millis(ms),
                    burst: wire.burst,
                },
                None => BudgetStrategy::Unknown { kind: wire.kind },
            },
            "while_in_flight" => BudgetStrategy::WhileInFlight,
            _ => BudgetStrategy::Unknown { kind: wire.kind },
        }
    }
}

impl From<BudgetStrategy> for StrategyWire {
    fn from(strategy: BudgetStrategy) -> Self {
        match strategy {
            BudgetStrategy::TimeBased { duration, burst } => StrategyWire {
                kind: "time_based".into(),
                duration_ms: Some(duration.as_millis() as u64),
                burst,
            },
            BudgetStrategy::WhileInFlight => StrategyWire {
                kind: "while_in_flight".into(),
                duration_ms: None,
                burst: None,
            },
            BudgetStrategy::Unknown { kind } => StrategyWire {
                kind,
                duration_ms: None,
                burst: None,
            },
        }
    }
}

/// A budget's policy — everything needed to create one.
///
/// ```
/// use std::time::Duration;
/// use zizq::{BudgetPolicy, BudgetStrategy};
///
/// let policy = BudgetPolicy::new(10_000, BudgetStrategy::time_based(Duration::from_secs(3600)));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetPolicy {
    /// Tokens the pool holds when full.
    pub allocation: u32,

    /// How those tokens replenish.
    pub strategy: BudgetStrategy,
}

impl BudgetPolicy {
    /// A policy with the given allocation and strategy.
    ///
    /// `const`, so it can be used in a [`JobKind::BUDGETS`] entry via
    /// [`BudgetBindingInput::create_with`].
    ///
    /// [`JobKind::BUDGETS`]: crate::JobKind::BUDGETS
    pub const fn new(allocation: u32, strategy: BudgetStrategy) -> Self {
        Self {
            allocation,
            strategy,
        }
    }
}

/// A budget as returned by the server.
///
/// Timestamps are Unix milliseconds since the epoch.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Budget {
    /// The budget's key.
    pub key: String,

    /// Tokens the pool holds when full.
    pub allocation: u32,

    /// How those tokens replenish.
    pub strategy: BudgetStrategy,

    /// When the budget was created.
    pub created_at: u64,

    /// When the policy was last changed.
    pub updated_at: u64,
}

/// Envelope for `GET /budgets`.
#[derive(Deserialize)]
pub(crate) struct ListBudgetsResponse {
    pub(crate) budgets: Vec<Budget>,
}

/// One budget a job draws on, as reported by the server.
///
/// Distinct from [`BudgetBindingInput`] because the two are not the
/// same shape: an input may omit `cost` and may carry `create_with`,
/// neither of which survives into what the job actually holds.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BudgetBinding {
    /// Key of the budget this job draws on.
    pub key: String,

    /// Tokens debited from that budget when the job dispatches.
    pub cost: u32,
}

/// A budget to bind a job to.
///
/// The key is a [`Cow`] so that one type serves both a per-type
/// default, fixed at compile time, and a binding built at runtime:
///
/// ```
/// use zizq::{BudgetBindingInput, BudgetPolicy, BudgetStrategy};
///
/// const BUDGETS: &[BudgetBindingInput] = &[
///     BudgetBindingInput::new("emails").cost(2),
///     BudgetBindingInput::with_policy(
///         "stripe",
///         BudgetPolicy::new(3, BudgetStrategy::WhileInFlight),
///     ),
/// ];
///
/// // A key computed at runtime works too.
/// let tenant = format!("emails:{}", "acme");
/// let binding = BudgetBindingInput::from(tenant).cost(2);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BudgetBindingInput {
    /// Key of the budget to draw on.
    pub key: Cow<'static, str>,

    /// Tokens to debit when the job dispatches. `None` means the
    /// server's default of `1`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost: Option<u32>,

    /// Policy to create the budget with if it does not already exist,
    /// atomically with the enqueue.
    ///
    /// If the budget does exist this is **ignored** and the stored
    /// policy stays authoritative, so an enqueue cannot accidentally
    /// clobber an existing budget.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_with: Option<BudgetPolicy>,
}

impl BudgetBindingInput {
    /// A binding to `key` at the server's default cost.
    pub const fn new(key: &'static str) -> Self {
        Self {
            key: Cow::Borrowed(key),
            cost: None,
            create_with: None,
        }
    }

    /// Set the tokens this job debits from the budget.
    pub const fn cost(mut self, cost: u32) -> Self {
        self.cost = Some(cost);
        self
    }

    /// A binding to `key` that creates the budget with `policy` if it
    /// does not already exist.
    ///
    /// The `const` counterpart to [`create_with`](Self::create_with),
    /// which cannot be `const` itself.
    pub const fn with_policy(key: &'static str, policy: BudgetPolicy) -> Self {
        Self {
            key: Cow::Borrowed(key),
            cost: None,
            create_with: Some(policy),
        }
    }

    /// Create the budget with this policy if it does not exist yet.
    ///
    /// Unlike the other setters this is not `const`, and cannot be:
    /// overwriting the existing `Option<BudgetPolicy>` drops it, and a
    /// `const fn` may not run a destructor. Use
    /// [`with_policy`](Self::with_policy) in a `const` context, which
    /// constructs rather than overwrites and so drops nothing.
    pub fn create_with(mut self, policy: BudgetPolicy) -> Self {
        self.create_with = Some(policy);
        self
    }
}

impl From<&'static str> for BudgetBindingInput {
    fn from(key: &'static str) -> Self {
        Self::new(key)
    }
}

impl From<String> for BudgetBindingInput {
    fn from(key: String) -> Self {
        Self {
            key: Cow::Owned(key),
            cost: None,
            create_with: None,
        }
    }
}

/// Changes to apply to an existing budget's policy.
///
/// A JSON Merge Patch, so a field no setter touched is left unchanged
/// on the server and a freshly constructed `BudgetPatch` changes
/// nothing. See the [`job_patch`](crate::JobPatch) module docs for the
/// keep / clear / set model this shares.
///
/// ```
/// use std::time::Duration;
/// use zizq::{BudgetPatch, BudgetStrategy};
///
/// // Cap the spike without restating the rate.
/// let tighten = BudgetPatch::new().burst(500);
///
/// // Let it spike again, up to the full allocation.
/// let loosen = BudgetPatch::new().clear_burst();
///
/// // Change the policy wholesale.
/// let swap = BudgetPatch::new()
///     .allocation(20_000)
///     .strategy(BudgetStrategy::time_based(Duration::from_secs(3600)));
/// ```
#[derive(Debug, Clone, Default, Serialize)]
pub struct BudgetPatch {
    /// New allocation. The server forbids a null allocation — a budget
    /// without one is not a budget — so this is only ever keep or set.
    #[serde(skip_serializing_if = "Field::is_keep")]
    allocation: Field<u32>,

    /// Strategy changes, itself a sub-field merge patch.
    #[serde(skip_serializing_if = "Field::is_keep")]
    strategy: Field<StrategyPatch>,
}

/// A merge patch over a budget's strategy. Built through
/// [`BudgetPatch`]'s setters rather than directly, so that the
/// combinations the server rejects cannot be assembled.
#[derive(Debug, Clone, Default, Serialize)]
struct StrategyPatch {
    #[serde(rename = "type", skip_serializing_if = "Field::is_keep")]
    kind: Field<String>,

    #[serde(skip_serializing_if = "Field::is_keep")]
    duration_ms: Field<u64>,

    #[serde(skip_serializing_if = "Field::is_keep")]
    burst: Field<u32>,
}

impl BudgetPatch {
    /// A patch that changes nothing. Chain setters to fill it in.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of tokens the pool holds when full.
    pub fn allocation(mut self, allocation: u32) -> Self {
        self.allocation = Field::Set(allocation);
        self
    }

    /// Replace the strategy outright.
    ///
    /// Every field the new strategy implies is stated, so the result is
    /// exactly the strategy given rather than a merge with whatever was
    /// there before. In particular a [`TimeBased`] with no burst clears
    /// the stored one instead of inheriting it.
    ///
    /// [`TimeBased`]: BudgetStrategy::TimeBased
    pub fn strategy(mut self, strategy: BudgetStrategy) -> Self {
        self.strategy = Field::Set(match strategy {
            BudgetStrategy::TimeBased { duration, burst } => StrategyPatch {
                kind: Field::Set("time_based".into()),
                duration_ms: Field::Set(duration.as_millis() as u64),
                burst: match burst {
                    Some(burst) => Field::Set(burst),
                    None => Field::Clear,
                },
            },
            // Neither a period nor a burst is emitted: the server
            // rejects either alongside a switch to `while_in_flight`,
            // and drops the stored ones itself.
            BudgetStrategy::WhileInFlight => StrategyPatch {
                kind: Field::Set("while_in_flight".into()),
                ..StrategyPatch::default()
            },
            BudgetStrategy::Unknown { kind } => StrategyPatch {
                kind: Field::Set(kind),
                ..StrategyPatch::default()
            },
        });
        self
    }

    /// Set the period over which the allocation replenishes, leaving
    /// the rest of the strategy alone.
    pub fn duration(mut self, duration: Duration) -> Self {
        self.strategy_mut().duration_ms = Field::Set(duration.as_millis() as u64);
        self
    }

    /// Cap how full the pool may get, leaving the rest of the strategy
    /// alone.
    pub fn burst(mut self, burst: u32) -> Self {
        self.strategy_mut().burst = Field::Set(burst);
        self
    }

    /// Clear the burst ceiling back to the allocation.
    pub fn clear_burst(mut self) -> Self {
        self.strategy_mut().burst = Field::Clear;
        self
    }

    /// The nested strategy patch, promoted from keep to set on first
    /// use so that a setter for one strategy field does not have to
    /// restate the others.
    fn strategy_mut(&mut self) -> &mut StrategyPatch {
        if self.strategy.is_keep() {
            self.strategy = Field::Set(StrategyPatch::default());
        }
        match &mut self.strategy {
            Field::Set(strategy) => strategy,
            _ => unreachable!("strategy was just promoted to Field::Set"),
        }
    }
}

/// The outcome of changing budget bindings across a selection of jobs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BudgetChange {
    /// How many jobs were changed.
    pub changed: u64,

    /// Jobs that were in flight and so could not be changed.
    ///
    /// An in-flight job has already debited its tokens. These drain on
    /// their own, so this is effectively a retry list — the same call
    /// at a later time would pick them up.
    #[serde(default)]
    pub blocked: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(strategy: &BudgetStrategy) -> serde_json::Value {
        serde_json::to_value(strategy).unwrap()
    }

    fn parse(value: serde_json::Value) -> BudgetStrategy {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn time_based_serialises_duration_as_milliseconds() {
        assert_eq!(
            json(&BudgetStrategy::time_based(Duration::from_secs(60))),
            serde_json::json!({ "type": "time_based", "duration_ms": 60_000 })
        );
    }

    #[test]
    fn burst_is_omitted_when_unset() {
        let with_burst = BudgetStrategy::TimeBased {
            duration: Duration::from_secs(60),
            burst: Some(5),
        };

        assert_eq!(
            json(&with_burst),
            serde_json::json!({ "type": "time_based", "duration_ms": 60_000, "burst": 5 })
        );
        assert_eq!(
            json(&BudgetStrategy::time_based(Duration::from_secs(60)))["burst"],
            serde_json::Value::Null
        );
    }

    // The server rejects a `while_in_flight` budget carrying either
    // field, so emitting one would be a request that reads as if it
    // set a refill period but did not.
    #[test]
    fn while_in_flight_carries_neither_duration_nor_burst() {
        assert_eq!(
            json(&BudgetStrategy::WhileInFlight),
            serde_json::json!({ "type": "while_in_flight" })
        );
    }

    #[test]
    fn strategies_round_trip() {
        for strategy in [
            BudgetStrategy::WhileInFlight,
            BudgetStrategy::time_based(Duration::from_secs(3600)),
            BudgetStrategy::TimeBased {
                duration: Duration::from_millis(1500),
                burst: Some(500),
            },
        ] {
            assert_eq!(parse(json(&strategy)), strategy);
        }
    }

    #[test]
    fn unrecognised_strategy_decodes_rather_than_failing() {
        assert_eq!(
            parse(serde_json::json!({ "type": "sliding_window", "duration_ms": 60_000 })),
            BudgetStrategy::Unknown {
                kind: "sliding_window".into()
            }
        );
    }

    // A rate with no period is not a rate. Defaulting it would invent
    // a limit the server never set.
    #[test]
    fn time_based_without_a_duration_is_unknown() {
        assert_eq!(
            parse(serde_json::json!({ "type": "time_based" })),
            BudgetStrategy::Unknown {
                kind: "time_based".into()
            }
        );
    }

    #[test]
    fn unknown_writes_back_the_name_the_server_gave() {
        assert_eq!(
            json(&BudgetStrategy::Unknown {
                kind: "sliding_window".into()
            }),
            serde_json::json!({ "type": "sliding_window" })
        );
    }

    #[test]
    fn budget_decodes_from_the_server_shape() {
        let budget: Budget = serde_json::from_value(serde_json::json!({
            "key": "emails",
            "allocation": 100,
            "strategy": { "type": "time_based", "duration_ms": 60_000, "burst": 5 },
            "created_at": 1_700_000_000_000u64,
            "updated_at": 1_700_000_001_000u64,
        }))
        .unwrap();

        assert_eq!(budget.key, "emails");
        assert_eq!(budget.allocation, 100);
        assert_eq!(
            budget.strategy,
            BudgetStrategy::TimeBased {
                duration: Duration::from_secs(60),
                burst: Some(5),
            }
        );
        assert_eq!(budget.created_at, 1_700_000_000_000);
        assert_eq!(budget.updated_at, 1_700_000_001_000);
    }

    #[test]
    fn binding_input_omits_what_was_never_set() {
        assert_eq!(
            serde_json::to_value(BudgetBindingInput::new("emails")).unwrap(),
            serde_json::json!({ "key": "emails" })
        );
    }

    #[test]
    fn binding_input_carries_cost_and_policy() {
        let binding = BudgetBindingInput::new("emails")
            .cost(2)
            .create_with(BudgetPolicy::new(100, BudgetStrategy::WhileInFlight));

        assert_eq!(
            serde_json::to_value(binding).unwrap(),
            serde_json::json!({
                "key": "emails",
                "cost": 2,
                "create_with": {
                    "allocation": 100,
                    "strategy": { "type": "while_in_flight" },
                },
            })
        );
    }

    // The whole point of the Cow: one type for a compile-time constant
    // and a key computed at runtime.
    #[test]
    fn binding_input_accepts_borrowed_and_owned_keys() {
        const CONST_BINDING: BudgetBindingInput = BudgetBindingInput::new("emails").cost(2);

        let owned = BudgetBindingInput::from(format!("emails:{}", "acme"));

        assert_eq!(CONST_BINDING.key, "emails");
        assert_eq!(CONST_BINDING.cost, Some(2));
        assert_eq!(owned.key, "emails:acme");
    }

    // A `const` item, so this is checked at compile time: everything a
    // per-type default needs is reachable without a destructor
    // running. `create_with` cannot be, which is what `with_policy`
    // exists for.
    #[test]
    fn a_binding_carrying_a_policy_is_const_constructible() {
        const BUDGETS: &[BudgetBindingInput] = &[
            BudgetBindingInput::new("emails").cost(2),
            BudgetBindingInput::with_policy(
                "stripe",
                BudgetPolicy::new(3, BudgetStrategy::WhileInFlight),
            ),
        ];

        assert_eq!(BUDGETS[1].key, "stripe");
        assert_eq!(
            BUDGETS[1].create_with,
            Some(BudgetPolicy::new(3, BudgetStrategy::WhileInFlight))
        );
        assert_eq!(BUDGETS[1].cost, None);
    }

    #[test]
    fn budget_change_decodes_and_defaults_blocked() {
        let change: BudgetChange =
            serde_json::from_value(serde_json::json!({ "changed": 12, "blocked": ["01K9"] }))
                .unwrap();
        assert_eq!(change.changed, 12);
        assert_eq!(change.blocked, vec!["01K9".to_string()]);

        let empty: BudgetChange =
            serde_json::from_value(serde_json::json!({ "changed": 0 })).unwrap();
        assert!(empty.blocked.is_empty());
    }
}
