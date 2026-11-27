import * as Schema from "effect/Schema";

export const ReleaseIso = Schema.String.check(
  Schema.isMaxLength(64),
  Schema.makeFilter((value) => {
    const m =
      /^(\d{4})-(\d{2})-(\d{2})T([01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.exec(
        value,
      );
    if (!m || !Number.isFinite(Date.parse(value))) return false;
    const year = Number(m[1]),
      month = Number(m[2]),
      day = Number(m[3]);
    const days = [
      31,
      year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0) ? 29 : 28,
      31,
      30,
      31,
      30,
      31,
      31,
      30,
      31,
      30,
      31,
    ];
    return month >= 1 && month <= 12 && day >= 1 && day <= days[month - 1]!;
  }),
);
export const ReleaseId = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(200),
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:/-]*$/),
);
export const ReleaseHash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export const ReleaseGitSha = Schema.String.check(
  Schema.isPattern(/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/),
);
export const ReleaseCount = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
);
export const ReleasePositive = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
);
export const ReleaseText = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(1024),
  Schema.isPattern(/^[^\x00-\x1f\x7f]+$/),
);
