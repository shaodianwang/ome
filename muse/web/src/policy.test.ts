import { describe, expect, it } from "vitest";

import { lowerThresholdTone, upperThresholdTone } from "./policy";

describe("dashboard health policy", () => {
  it("classifies upper-bound warning and critical boundaries", () => {
    const policy = { warningAt: 75, criticalAt: 90 };

    expect(upperThresholdTone(74.9, policy)).toBe("green");
    expect(upperThresholdTone(75, policy)).toBe("amber");
    expect(upperThresholdTone(90, policy)).toBe("red");
  });

  it("classifies lower-bound warning and critical boundaries", () => {
    const policy = { warningBelow: 0.7, criticalBelow: 0.4 };

    expect(lowerThresholdTone(0.7, policy)).toBe("green");
    expect(lowerThresholdTone(0.4, policy)).toBe("amber");
    expect(lowerThresholdTone(0.39, policy)).toBe("red");
  });

  it("classifies unrounded availability against percentage thresholds", () => {
    const policy = { warningBelow: 100, criticalBelow: 99 };

    expect(lowerThresholdTone(99.5, policy)).toBe("amber");
  });

  it("renders neutral status when policy or data is absent", () => {
    expect(upperThresholdTone(99, undefined)).toBe("");
    expect(lowerThresholdTone(null, { warningBelow: 1, criticalBelow: 0.9 })).toBe("");
  });
});
