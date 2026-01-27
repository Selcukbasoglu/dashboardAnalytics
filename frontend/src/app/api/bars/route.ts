import { proxyJSON } from "@/app/api/_proxy";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8080";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const assets = searchParams.get("assets") ?? "";
  const limit = searchParams.get("limit") ?? "";
  const qs = new URLSearchParams();
  if (assets) qs.set("assets", assets);
  if (limit) qs.set("limit", limit);
  const url = `${API_BASE}/api/v1/bars?${qs.toString()}`;
  const { status, data } = await proxyJSON(url);
  return Response.json(data, { status });
}
