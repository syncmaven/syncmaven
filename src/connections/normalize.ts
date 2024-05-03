import crypto from "crypto";

export function emailHash(email: string): string {
  return crypto.createHash("sha256").update(email).digest("hex")
}

export function normalizeEmail(email: string): string {
  return email.toLowerCase().trim()
}