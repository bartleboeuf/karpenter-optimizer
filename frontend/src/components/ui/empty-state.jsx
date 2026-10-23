import * as React from "react"

import { cn } from "../../lib/utils"

// EmptyState is the consistent "no data" panel: icon + title + hint, with an
// optional retry button. Replaces the ad-hoc "No X found" strings across tabs.
function EmptyState({ icon: Icon, title, hint, onRetry, retryLabel = "Retry", className, children }) {
  return (
    <div
      role="status"
      className={cn(
        "flex flex-col items-center justify-center rounded-lg border border-dashed px-6 py-12 text-center",
        className
      )}
    >
      {Icon && (
        <div className="mb-3 flex h-10 w-10 items-center justify-center rounded-full bg-muted">
          <Icon className="h-5 w-5 text-muted-foreground" aria-hidden="true" />
        </div>
      )}
      <p className="text-sm font-medium text-foreground">{title}</p>
      {hint && <p className="mt-1 max-w-sm text-xs text-muted-foreground">{hint}</p>}
      {onRetry && (
        <button
          type="button"
          onClick={onRetry}
          className="mt-4 inline-flex items-center gap-1.5 rounded-md border bg-card px-3 py-1.5 text-xs font-medium shadow-sm transition-colors hover:bg-muted"
        >
          {retryLabel}
        </button>
      )}
      {children}
    </div>
  )
}

export { EmptyState }
