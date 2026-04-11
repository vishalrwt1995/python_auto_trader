"use client";

import { useState, useMemo } from "react";
import { cn } from "@/lib/utils";
import { ChevronsUpDown, ChevronUp, ChevronDown, SearchX } from "lucide-react";
import { InfoBadge } from "@/components/shared/Tooltip";

export interface Column<T> {
  key: string;
  label: string;
  tooltip?: string;
  sortable?: boolean;
  /** Applied to <td> only — not leaked into <th> to avoid header styling conflicts */
  className?: string;
  render: (row: T, index: number) => React.ReactNode;
  sortValue?: (row: T) => string | number;
}

interface Props<T> {
  columns: Column<T>[];
  data: T[];
  onRowClick?: (row: T) => void;
  emptyMessage?: string;
  maxHeight?: string;
  rowClassName?: (row: T, index: number) => string;
}

export function DataTable<T>({
  columns,
  data,
  onRowClick,
  emptyMessage = "No data",
  maxHeight = "calc(100vh - 280px)",
  rowClassName,
}: Props<T>) {
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  const sorted = useMemo(() => {
    if (!sortKey) return data;
    const col = columns.find((c) => c.key === sortKey);
    if (!col?.sortValue) return data;
    const fn = col.sortValue;
    return [...data].sort((a, b) => {
      const av = fn(a);
      const bv = fn(b);
      if (av < bv) return sortDir === "asc" ? -1 : 1;
      if (av > bv) return sortDir === "asc" ? 1 : -1;
      return 0;
    });
  }, [data, sortKey, sortDir, columns]);

  const toggleSort = (key: string) => {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  };

  if (data.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 gap-3">
        <div className="p-4 rounded-full bg-bg-tertiary/50">
          <SearchX className="h-6 w-6 text-text-secondary" />
        </div>
        <p className="text-sm text-text-secondary">{emptyMessage}</p>
      </div>
    );
  }

  return (
    <div
      className="overflow-auto scrollbar-thin rounded-xl border border-bg-tertiary"
      style={{ maxHeight, boxShadow: "0 4px 24px rgba(0,0,0,0.3)" }}
    >
      <table className="w-full text-sm">
        <thead
          className="sticky top-0 z-10"
          style={{
            background: "rgba(17,24,39,0.85)",
            backdropFilter: "blur(12px)",
            WebkitBackdropFilter: "blur(12px)",
            borderBottom: "1px solid rgba(31,41,55,0.8)",
          }}
        >
          <tr>
            {columns.map((col) => (
              <th
                key={col.key}
                className={cn(
                  "px-3 py-2.5 text-left text-xs font-semibold text-text-secondary whitespace-nowrap tracking-wide uppercase",
                  col.sortable && "cursor-pointer select-none hover:text-text-primary transition-colors",
                )}
                onClick={() => col.sortable && toggleSort(col.key)}
              >
                <span className="inline-flex items-center gap-1.5">
                  {col.label}
                  {col.sortable && (
                    <span className="opacity-50">
                      {sortKey === col.key ? (
                        sortDir === "asc"
                          ? <ChevronUp className="h-3 w-3 opacity-100 text-accent" />
                          : <ChevronDown className="h-3 w-3 opacity-100 text-accent" />
                      ) : (
                        <ChevronsUpDown className="h-3 w-3" />
                      )}
                    </span>
                  )}
                  {col.tooltip && <InfoBadge text={col.tooltip} />}
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((row, i) => (
            <tr
              key={i}
              className={cn(
                "border-t border-bg-tertiary/40 transition-all duration-100",
                i % 2 === 1 && "bg-bg-tertiary/10",
                onRowClick
                  ? "cursor-pointer hover:bg-accent/5 hover:border-accent/10"
                  : "hover:bg-bg-tertiary/20",
                rowClassName?.(row, i),
              )}
              onClick={() => onRowClick?.(row)}
            >
              {columns.map((col) => (
                <td
                  key={col.key}
                  className={cn("px-3 py-2 whitespace-nowrap", col.className)}
                >
                  {col.render(row, i)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
