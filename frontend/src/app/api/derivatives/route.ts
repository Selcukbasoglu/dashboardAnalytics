import { NextResponse } from "next/server";
import { readJSON } from "@/app/api/_proxy";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8080";

export async function GET(req: Request) {
  const { search } = new URL(req.url);
  const headers = new Headers();
  const ifNoneMatch = req.headers.get("if-none-match");
  const ifModifiedSince = req.headers.get("if-modified-since");
  if (ifNoneMatch) headers.set("If-None-Match", ifNoneMatch);
  if (ifModifiedSince) headers.set("If-Modified-Since", ifModifiedSince);

  let res: Response;
  try {
    res = await fetch(`${API_BASE}/api/v1/derivatives${search}`, { headers, cache: "no-store" });
  } catch (err) {
    return NextResponse.json({ error: "upstream_unreachable", detail: String(err) }, { status: 502 });
  }
  const etag = res.headers.get("ETag");
  const lastModified = res.headers.get("Last-Modified");
  const outHeaders = new Headers();
  if (etag) outHeaders.set("ETag", etag);
  if (lastModified) outHeaders.set("Last-Modified", lastModified);

  if (res.status === 304) {
    return new Response(null, { status: 304, headers: outHeaders });
  }

  const { data } = await readJSON(res);
  return NextResponse.json(data, { status: res.status, headers: outHeaders });
}
