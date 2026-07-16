import type { LowerThresholdPolicy, UpperThresholdPolicy } from "./api";
import type { BarTone } from "./types";

/** Classify a metric where larger values are worse. Missing policy is neutral. */
export function upperThresholdTone(
  value: number | null | undefined,
  policy: UpperThresholdPolicy | undefined,
): BarTone {
  if (value == null || !policy) return "";
  if (value >= policy.criticalAt) return "red";
  if (value >= policy.warningAt) return "amber";
  return "green";
}

/** Classify a metric where smaller values are worse. Missing policy is neutral. */
export function lowerThresholdTone(
  value: number | null | undefined,
  policy: LowerThresholdPolicy | undefined,
): BarTone {
  if (value == null || !policy) return "";
  if (value < policy.criticalBelow) return "red";
  if (value < policy.warningBelow) return "amber";
  return "green";
}
