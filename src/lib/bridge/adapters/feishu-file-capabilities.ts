/**
 * Feishu file/image capabilities — B1 probing + B2 send helpers.
 *
 * Separated from feishu-adapter.ts to keep concerns clean:
 * - probeCapabilities() — test whether the bot has im:image / im:file permissions
 * - sendImage() / sendFile() — upload then send via im.message.create
 * - buildPermissionGuide() — human-readable missing-permission message
 *
 * These functions accept a lark.Client instance (or compatible mock) so they
 * can be unit-tested without the full adapter lifecycle.
 */

import path from 'path';
import fs from 'fs';
import { Readable } from 'stream';
import type { SendResult } from '../types.js';

// ── Types ────────────────────────────────────────────────────

export interface FeishuCapabilities {
  canSendImage: boolean;
  canSendFile: boolean;
}

/**
 * Minimal shape of the lark SDK client that we depend on.
 * Using a structural type so unit tests can pass a plain mock object.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyClient = any;

// ── File type mapping ────────────────────────────────────────

const FILE_TYPE_MAP: Record<string, string> = {
  pdf: 'pdf',
  doc: 'doc',
  docx: 'doc',
  xls: 'xls',
  xlsx: 'xls',
  ppt: 'ppt',
  pptx: 'ppt',
  mp4: 'mp4',
  opus: 'opus',
  ogg: 'opus',
};

/** Map a file extension to a Feishu file_type value. */
export function mapFileType(ext: string): string {
  return FILE_TYPE_MAP[ext.toLowerCase()] || 'stream';
}

// ── Receive ID type helper ───────────────────────────────────

function resolveReceiveIdType(chatId: string): string {
  return chatId.startsWith('ou_') ? 'open_id' : 'chat_id';
}

// ── B1: Capability probing ──────────────────────────────────

/** 1x1 transparent PNG as base64 */
const PROBE_PNG_B64 =
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==';

/**
 * Probe whether the bot has image/file upload permissions by attempting
 * a minimal upload. Does NOT throw — returns capability flags.
 */
export async function probeCapabilities(
  client: AnyClient,
): Promise<FeishuCapabilities> {
  const result: FeishuCapabilities = {
    canSendImage: false,
    canSendFile: false,
  };

  // Probe image upload
  try {
    const buf = Buffer.from(PROBE_PNG_B64, 'base64');
    const stream = Readable.from(buf);
    await client.im.image.create({
      data: { image_type: 'message', image: stream },
    });
    result.canSendImage = true;
  } catch (err: unknown) {
    const code = (err as { code?: number })?.code;
    console.warn(
      '[feishu-capabilities] Image probe failed:',
      code || (err instanceof Error ? err.message : err),
    );
  }

  // Probe file upload
  try {
    const stream = Readable.from(Buffer.from('probe'));
    await client.im.file.create({
      data: { file_type: 'stream', file_name: 'probe.txt', file: stream },
    });
    result.canSendFile = true;
  } catch (err: unknown) {
    const code = (err as { code?: number })?.code;
    console.warn(
      '[feishu-capabilities] File probe failed:',
      code || (err instanceof Error ? err.message : err),
    );
  }

  return result;
}

// ── Permission guide ────────────────────────────────────────

/**
 * Build a human-readable message listing missing permissions.
 * Returns null if all capabilities are available.
 */
export function buildPermissionGuide(caps: FeishuCapabilities): string | null {
  const missing: string[] = [];
  if (!caps.canSendImage)
    missing.push('im:image — im:image:create 权限（发送图片）');
  if (!caps.canSendFile)
    missing.push('im:file — im:file:create 权限（发送文件）');
  if (missing.length === 0) return null;
  return [
    '飞书 Bot 权限不完整，以下功能不可用：',
    ...missing.map((m) => `  - ${m}`),
    '',
    '请在飞书开发者后台 → 应用权限 中添加以上权限。',
  ].join('\n');
}

// ── B2: sendImage ───────────────────────────────────────────

/**
 * Upload an image then send it as an image message.
 */
export async function sendImage(
  client: AnyClient,
  chatId: string,
  filePath: string,
): Promise<SendResult> {
  try {
    const imageRes = await client.im.image.create({
      data: { image_type: 'message', image: fs.createReadStream(filePath) },
    });
    const imageKey = imageRes?.data?.image_key;
    if (!imageKey) return { ok: false, error: 'Failed to upload image' };

    const receiveIdType = resolveReceiveIdType(chatId);
    const msgRes = await client.im.message.create({
      params: { receive_id_type: receiveIdType },
      data: {
        receive_id: chatId,
        msg_type: 'image',
        content: JSON.stringify({ image_key: imageKey }),
      },
    });
    return { ok: true, messageId: msgRes?.data?.message_id };
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}

// ── B2: sendFile ────────────────────────────────────────────

/**
 * Upload a file then send it as a file message.
 */
export async function sendFile(
  client: AnyClient,
  chatId: string,
  filePath: string,
): Promise<SendResult> {
  try {
    const ext = path.extname(filePath).slice(1);
    const fileType = mapFileType(ext);
    const fileName = path.basename(filePath);

    const fileRes = await client.im.file.create({
      data: {
        file_type: fileType,
        file_name: fileName,
        file: fs.createReadStream(filePath),
      },
    });
    const fileKey = fileRes?.data?.file_key;
    if (!fileKey) return { ok: false, error: 'Failed to upload file' };

    const receiveIdType = resolveReceiveIdType(chatId);
    const msgRes = await client.im.message.create({
      params: { receive_id_type: receiveIdType },
      data: {
        receive_id: chatId,
        msg_type: 'file',
        content: JSON.stringify({ file_key: fileKey }),
      },
    });
    return { ok: true, messageId: msgRes?.data?.message_id };
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}
