import * as React from "react"

import { cn } from "../../lib/utils"

// Skeleton is a simple pulse placeholder (radix-free) shown while a tab
// loads its data. Use it to mirror the shape of the content it stands in for.
const Skeleton = React.forwardRef(({ className, ...props }, ref) => (
  <div
    ref={ref}
    className={cn("animate-pulse rounded-md bg-muted", className)}
    {...props}
  />
))
Skeleton.displayName = "Skeleton"

// SkeletonRows renders a small stack of skeleton bars, a lightweight stand-in
// for a list/table while the data is in flight.
function SkeletonRows({ rows = 5, className }) {
  return (
    <div className={cn("space-y-3", className)} role="status" aria-label="Loading">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="space-y-2">
          <Skeleton className="h-4 w-1/3" />
          <Skeleton className="h-3 w-2/3" />
        </div>
      ))}
    </div>
  )
}

export { Skeleton, SkeletonRows }
