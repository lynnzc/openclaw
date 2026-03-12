/**
 * Tests for the double-announce bug in cron delivery dispatch.
 *
 * Bug: early return paths in deliverViaAnnounce (active subagent suppression
 * and stale interim message suppression) returned without setting
 * deliveryAttempted = true. The timer saw deliveryAttempted = false and
 * fired enqueueSystemEvent as a fallback, causing a second announcement.
 *
 * Fix: both early return paths now set deliveryAttempted = true before
 * returning so the timer correctly skips the system-event fallback.
 */

import { beforeEach, describe, expect, it, vi } from "vitest";

// --- Module mocks (must be hoisted before imports) ---

vi.mock("../../agents/subagent-announce.js", () => ({
  runSubagentAnnounceFlow: vi.fn().mockResolvedValue(true),
}));

vi.mock("../../agents/subagent-registry.js", () => ({
  listDescendantRunsForRequester: vi.fn().mockReturnValue([]),
}));

vi.mock("../../config/sessions.js", () => ({
  resolveAgentMainSessionKey: vi.fn().mockReturnValue("agent:main"),
}));

vi.mock("../../infra/outbound/outbound-session.js", () => ({
  resolveOutboundSessionRoute: vi.fn().mockResolvedValue(null),
  ensureOutboundSessionEntry: vi.fn().mockResolvedValue(undefined),
}));

vi.mock("../../infra/outbound/deliver.js", () => ({
  deliverOutboundPayloads: vi.fn().mockResolvedValue([{ ok: true }]),
}));

vi.mock("../../infra/outbound/identity.js", () => ({
  resolveAgentOutboundIdentity: vi.fn().mockReturnValue({}),
}));

vi.mock("../../infra/outbound/session-context.js", () => ({
  buildOutboundSessionContext: vi.fn().mockReturnValue({}),
}));

vi.mock("../../cli/outbound-send-deps.js", () => ({
  createOutboundSendDeps: vi.fn().mockReturnValue({}),
}));

vi.mock("../../logger.js", () => ({
  logWarn: vi.fn(),
}));

vi.mock("./subagent-followup.js", () => ({
  expectsSubagentFollowup: vi.fn().mockReturnValue(false),
  isLikelyInterimCronMessage: vi.fn().mockReturnValue(false),
  readDescendantSubagentFallbackReply: vi.fn().mockResolvedValue(undefined),
  waitForDescendantSubagentSummary: vi.fn().mockResolvedValue(undefined),
}));

import { runSubagentAnnounceFlow } from "../../agents/subagent-announce.js";
// Import after mocks
import { listDescendantRunsForRequester } from "../../agents/subagent-registry.js";
import { shouldEnqueueCronMainSummary } from "../heartbeat-policy.js";
import { dispatchCronDelivery } from "./delivery-dispatch.js";
import type { DeliveryTargetResolution } from "./delivery-target.js";
import type { RunCronAgentTurnResult } from "./run.js";
import {
  expectsSubagentFollowup,
  isLikelyInterimCronMessage,
  readDescendantSubagentFallbackReply,
  waitForDescendantSubagentSummary,
} from "./subagent-followup.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeResolvedDelivery(): Extract<DeliveryTargetResolution, { ok: true }> {
  return {
    ok: true,
    channel: "telegram",
    to: "123456",
    accountId: undefined,
    threadId: undefined,
    mode: "explicit",
  };
}

function makeWithRunSession() {
  return (
    result: Omit<RunCronAgentTurnResult, "sessionId" | "sessionKey">,
  ): RunCronAgentTurnResult => ({
    ...result,
    sessionId: "test-session-id",
    sessionKey: "test-session-key",
  });
}

function makeBaseParams(overrides: { synthesizedText?: string; deliveryRequested?: boolean }) {
  const resolvedDelivery = makeResolvedDelivery();
  return {
    cfg: {} as never,
    cfgWithAgentDefaults: {} as never,
    deps: {} as never,
    job: {
      id: "test-job",
      name: "Test Job",
      deleteAfterRun: false,
      payload: { kind: "agentTurn", message: "hello" },
    } as never,
    agentId: "main",
    agentSessionKey: "agent:main",
    runSessionId: "run-123",
    runStartedAt: 10_000,
    runEndedAt: 12_000,
    timeoutMs: 30_000,
    resolvedDelivery,
    deliveryRequested: overrides.deliveryRequested ?? true,
    skipHeartbeatDelivery: false,
    skipMessagingToolDelivery: false,
    deliveryBestEffort: false,
    deliveryPayloadHasStructuredContent: false,
    deliveryPayloads: overrides.synthesizedText ? [{ text: overrides.synthesizedText }] : [],
    synthesizedText: overrides.synthesizedText ?? "on it",
    summary: overrides.synthesizedText ?? "on it",
    outputText: overrides.synthesizedText ?? "on it",
    telemetry: undefined,
    abortSignal: undefined,
    isAborted: () => false,
    abortReason: () => "aborted",
    withRunSession: makeWithRunSession(),
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("dispatchCronDelivery — double-announce guard", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(listDescendantRunsForRequester).mockReturnValue([]);
    vi.mocked(expectsSubagentFollowup).mockReturnValue(false);
    vi.mocked(isLikelyInterimCronMessage).mockReturnValue(false);
    vi.mocked(readDescendantSubagentFallbackReply).mockResolvedValue(undefined);
    vi.mocked(waitForDescendantSubagentSummary).mockResolvedValue(undefined);
    vi.mocked(runSubagentAnnounceFlow).mockResolvedValue(true);
  });

  it("early return (active subagent) sets deliveryAttempted=true so timer skips enqueueSystemEvent", async () => {
    // Active descendants in the current run window keep suppression active after waiting.
    vi.mocked(listDescendantRunsForRequester).mockReturnValue([
      {
        runId: "active-run",
        childSessionKey: "child-active",
        requesterSessionKey: "agent:main",
        requesterDisplayKey: "agent:main",
        task: "active task",
        cleanup: "keep",
        createdAt: 9_500,
        startedAt: 10_100,
      },
    ]);
    vi.mocked(waitForDescendantSubagentSummary).mockResolvedValue(undefined);
    vi.mocked(readDescendantSubagentFallbackReply).mockResolvedValue(undefined);

    const params = makeBaseParams({ synthesizedText: "on it" });
    const state = await dispatchCronDelivery(params);

    // deliveryAttempted must be true so timer does NOT fire enqueueSystemEvent
    expect(state.deliveryAttempted).toBe(true);

    // Verify timer guard agrees: shouldEnqueueCronMainSummary returns false
    expect(
      shouldEnqueueCronMainSummary({
        summaryText: "on it",
        deliveryRequested: true,
        delivered: state.delivered,
        deliveryAttempted: state.deliveryAttempted,
        suppressMainSummary: false,
        isCronSystemEvent: () => true,
      }),
    ).toBe(false);

    // No announce should have been attempted (subagents still running)
    expect(runSubagentAnnounceFlow).not.toHaveBeenCalled();
  });

  it("early return (stale interim suppression) sets deliveryAttempted=true so timer skips enqueueSystemEvent", async () => {
    // First check sees active descendants in this run; second check sees none.
    vi.mocked(listDescendantRunsForRequester)
      .mockReturnValueOnce([
        {
          runId: "active-run",
          childSessionKey: "child-active",
          requesterSessionKey: "agent:main",
          requesterDisplayKey: "agent:main",
          task: "active task",
          cleanup: "keep",
          createdAt: 9_000,
          startedAt: 10_200,
        },
      ])
      .mockReturnValueOnce([]);
    vi.mocked(waitForDescendantSubagentSummary).mockResolvedValue(undefined);
    vi.mocked(readDescendantSubagentFallbackReply).mockResolvedValue(undefined);
    // synthesizedText matches initialSynthesizedText & isLikelyInterimCronMessage → stale interim
    vi.mocked(isLikelyInterimCronMessage).mockReturnValue(true);

    const params = makeBaseParams({ synthesizedText: "on it, pulling everything together" });
    const state = await dispatchCronDelivery(params);

    // deliveryAttempted must be true so timer does NOT fire enqueueSystemEvent
    expect(state.deliveryAttempted).toBe(true);

    // Verify timer guard agrees
    expect(
      shouldEnqueueCronMainSummary({
        summaryText: "on it, pulling everything together",
        deliveryRequested: true,
        delivered: state.delivered,
        deliveryAttempted: state.deliveryAttempted,
        suppressMainSummary: false,
        isCronSystemEvent: () => true,
      }),
    ).toBe(false);

    // No announce or direct delivery should have been sent (stale interim suppressed)
    expect(runSubagentAnnounceFlow).not.toHaveBeenCalled();
  });

  it("normal announce success delivers exactly once and sets deliveryAttempted=true", async () => {
    vi.mocked(listDescendantRunsForRequester).mockReturnValue([]);
    vi.mocked(isLikelyInterimCronMessage).mockReturnValue(false);
    vi.mocked(runSubagentAnnounceFlow).mockResolvedValue(true);

    const params = makeBaseParams({ synthesizedText: "Morning briefing complete." });
    const state = await dispatchCronDelivery(params);

    expect(state.deliveryAttempted).toBe(true);
    expect(state.delivered).toBe(true);
    // Announce called exactly once
    expect(runSubagentAnnounceFlow).toHaveBeenCalledTimes(1);

    // Timer should not fire enqueueSystemEvent (delivered=true)
    expect(
      shouldEnqueueCronMainSummary({
        summaryText: "Morning briefing complete.",
        deliveryRequested: true,
        delivered: state.delivered,
        deliveryAttempted: state.deliveryAttempted,
        suppressMainSummary: false,
        isCronSystemEvent: () => true,
      }),
    ).toBe(false);
  });

  it("announce failure falls back to direct delivery exactly once (no double-deliver)", async () => {
    vi.mocked(listDescendantRunsForRequester).mockReturnValue([]);
    vi.mocked(isLikelyInterimCronMessage).mockReturnValue(false);
    // Announce fails: runSubagentAnnounceFlow returns false
    vi.mocked(runSubagentAnnounceFlow).mockResolvedValue(false);

    const { deliverOutboundPayloads } = await import("../../infra/outbound/deliver.js");
    vi.mocked(deliverOutboundPayloads).mockResolvedValue([{ ok: true } as never]);

    const params = makeBaseParams({ synthesizedText: "Briefing ready." });
    const state = await dispatchCronDelivery(params);

    // Delivery was attempted; direct fallback picked up the slack
    expect(state.deliveryAttempted).toBe(true);
    expect(state.delivered).toBe(true);

    // Announce was tried exactly once
    expect(runSubagentAnnounceFlow).toHaveBeenCalledTimes(1);

    // Direct fallback fired exactly once (not zero, not twice)
    // This ensures one delivery total reaches the user, not two
    expect(deliverOutboundPayloads).toHaveBeenCalledTimes(1);
  });

  it("ignores stale descendants outside run window and does not enter wait path", async () => {
    vi.mocked(listDescendantRunsForRequester).mockReturnValue([
      {
        runId: "stale-run",
        childSessionKey: "child-stale",
        requesterSessionKey: "agent:main",
        requesterDisplayKey: "agent:main",
        task: "stale task",
        cleanup: "keep",
        createdAt: 1_000,
        startedAt: 1_500,
      },
    ]);
    vi.mocked(isLikelyInterimCronMessage).mockReturnValue(false);
    vi.mocked(runSubagentAnnounceFlow).mockResolvedValue(true);

    const params = makeBaseParams({ synthesizedText: "Morning briefing complete." });
    const state = await dispatchCronDelivery(params);

    expect(waitForDescendantSubagentSummary).not.toHaveBeenCalled();
    expect(runSubagentAnnounceFlow).toHaveBeenCalledTimes(1);
    expect(state.delivered).toBe(true);
  });

  it("no delivery requested means deliveryAttempted stays false and runSubagentAnnounceFlow not called", async () => {
    const params = makeBaseParams({
      synthesizedText: "Task done.",
      deliveryRequested: false,
    });
    const state = await dispatchCronDelivery(params);

    expect(runSubagentAnnounceFlow).not.toHaveBeenCalled();
    // deliveryAttempted starts false (skipMessagingToolDelivery=false) and nothing runs
    expect(state.deliveryAttempted).toBe(false);
  });
});
