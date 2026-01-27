import { NextResponse } from "next/server";
import { proxyJSON } from "@/app/api/_proxy";

const PY_BASE = process.env.PY_INTEL_BASE_URL ?? "http://localhost:8001";

export async function GET() {
  const { status, data } = await proxyJSON(`${PY_BASE}/health`, { cache: "no-store" });
  return NextResponse.json(data, { status });
}
