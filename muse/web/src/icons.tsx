// Shared SVG icons, ported from the mockup (HTML attrs -> JSX camelCase).
// All inherit `currentColor`; sizing comes from the surrounding CSS.
import type { SVGProps } from "react";

type P = SVGProps<SVGSVGElement>;
const base = (p: P) => ({ viewBox: "0 0 24 24", fill: "none", ...p });

export const BrandMark = (p: P) => (
  <svg {...base(p)}>
    <path
      d="M3 17V7l5 4 4-6 4 6 5-4v10"
      stroke="#fff"
      strokeWidth={2}
      strokeLinejoin="round"
      strokeLinecap="round"
    />
  </svg>
);

export const IconFleet = (p: P) => (
  <svg {...base(p)}>
    <rect x="3" y="3" width="7" height="7" rx="1.5" stroke="currentColor" strokeWidth={1.7} />
    <rect x="14" y="3" width="7" height="7" rx="1.5" stroke="currentColor" strokeWidth={1.7} />
    <rect x="3" y="14" width="7" height="7" rx="1.5" stroke="currentColor" strokeWidth={1.7} />
    <rect x="14" y="14" width="7" height="7" rx="1.5" stroke="currentColor" strokeWidth={1.7} />
  </svg>
);

export const IconWorkloads = (p: P) => (
  <svg {...base(p)}>
    <path
      d="M4 6h16M4 12h16M4 18h16"
      stroke="currentColor"
      strokeWidth={1.7}
      strokeLinecap="round"
    />
    <circle cx="8" cy="6" r="1.4" fill="currentColor" />
    <circle cx="13" cy="12" r="1.4" fill="currentColor" />
    <circle cx="9" cy="18" r="1.4" fill="currentColor" />
  </svg>
);

export const IconModels = (p: P) => (
  <svg {...base(p)}>
    <path
      d="M12 3l8 4.5v9L12 21l-8-4.5v-9L12 3z"
      stroke="currentColor"
      strokeWidth={1.6}
      strokeLinejoin="round"
    />
    <path d="M12 3v9m0 0l8-4.5M12 12L4 7.5" stroke="currentColor" strokeWidth={1.4} opacity=".55" />
  </svg>
);

export const IconDeploy = (p: P) => (
  <svg {...base(p)}>
    <path
      d="M12 3v12m0-12l-4 4m4-4l4 4"
      stroke="currentColor"
      strokeWidth={1.7}
      strokeLinecap="round"
      strokeLinejoin="round"
    />
    <path
      d="M4 17v2a2 2 0 002 2h12a2 2 0 002-2v-2"
      stroke="currentColor"
      strokeWidth={1.7}
      strokeLinecap="round"
    />
  </svg>
);

export const IconCapacity = (p: P) => (
  <svg {...base(p)}>
    <path
      d="M4 19V9m5 10V4m5 15v-7m5 7V7"
      stroke="currentColor"
      strokeWidth={1.8}
      strokeLinecap="round"
    />
  </svg>
);

export const ChevronDown = (p: P) => (
  <svg {...base(p)}>
    <path d="M6 9l6 6 6-6" stroke="currentColor" strokeWidth={2} strokeLinecap="round" />
  </svg>
);

export const ChevronRight = (p: P) => (
  <svg {...base(p)}>
    <path d="M9 6l6 6-6 6" stroke="currentColor" strokeWidth={2} strokeLinecap="round" />
  </svg>
);

export const Search = (p: P) => (
  <svg {...base(p)}>
    <circle cx="11" cy="11" r="7" stroke="currentColor" strokeWidth={1.8} />
    <path d="M21 21l-4-4" stroke="currentColor" strokeWidth={1.8} strokeLinecap="round" />
  </svg>
);

export const Bell = (p: P) => (
  <svg {...base(p)}>
    <path
      d="M6 9a6 6 0 1112 0c0 5 2 6 2 6H4s2-1 2-6z"
      stroke="currentColor"
      strokeWidth={1.7}
      strokeLinejoin="round"
    />
    <path d="M10 21h4" stroke="currentColor" strokeWidth={1.7} strokeLinecap="round" />
  </svg>
);

export const Refresh = (p: P) => (
  <svg {...base(p)}>
    <path
      d="M4 12a8 8 0 018-8m8 8a8 8 0 01-8 8"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
    />
    <path d="M12 4l3-2m-3 18l-3 2" stroke="currentColor" strokeWidth={2} strokeLinecap="round" />
  </svg>
);

export const Plus = (p: P) => (
  <svg {...base(p)}>
    <path d="M12 5v14M5 12h14" stroke="currentColor" strokeWidth={2} strokeLinecap="round" />
  </svg>
);

export const ArrowRight = (p: P) => (
  <svg {...base(p)}>
    <path
      d="M5 12h14M13 6l6 6-6 6"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

export const Check = (p: P) => (
  <svg {...base(p)}>
    <path
      d="M5 13l4 4L19 7"
      stroke="currentColor"
      strokeWidth={2.2}
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

export const Columns = (p: P) => (
  <svg {...base(p)}>
    <path d="M3 5h18M6 12h12M10 19h4" stroke="currentColor" strokeWidth={2} strokeLinecap="round" />
  </svg>
);

export const Filter = (p: P) => (
  <svg {...base(p)}>
    <path
      d="M3 5h18l-7 8v6l-4 2v-8L3 5z"
      stroke="currentColor"
      strokeWidth={1.7}
      strokeLinejoin="round"
    />
  </svg>
);

export const Info = (p: P) => (
  <svg {...base(p)}>
    <circle cx="12" cy="12" r="9" stroke="currentColor" strokeWidth={1.7} />
    <path d="M12 8v5m0 3h.01" stroke="currentColor" strokeWidth={1.8} strokeLinecap="round" />
  </svg>
);

export const Spinner = (p: P) => (
  <svg {...base(p)}>
    <path d="M12 3a9 9 0 109 9" stroke="currentColor" strokeWidth={2.4} strokeLinecap="round" />
  </svg>
);

export const Warning = (p: P) => (
  <svg {...base(p)}>
    <path
      d="M12 9v4m0 4h.01M10.3 4.3L2.6 18a2 2 0 001.7 3h15.4a2 2 0 001.7-3L13.7 4.3a2 2 0 00-3.4 0z"
      stroke="currentColor"
      strokeWidth={1.7}
      strokeLinejoin="round"
    />
  </svg>
);

export const Lines = (p: P) => (
  <svg {...base(p)}>
    <path
      d="M4 7h16M4 12h16M4 17h10"
      stroke="currentColor"
      strokeWidth={1.8}
      strokeLinecap="round"
    />
  </svg>
);

export const Upload = (p: P) => (
  <svg {...base(p)}>
    <path
      d="M12 3v12m0-12l-4 4m4-4l4 4"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
    />
    <path
      d="M4 17v2a2 2 0 002 2h12a2 2 0 002-2v-2"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
    />
  </svg>
);
