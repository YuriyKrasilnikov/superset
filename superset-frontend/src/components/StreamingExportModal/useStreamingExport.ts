/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { useState, useCallback, useRef, useEffect } from 'react';
import { JsonObject, SupersetClient } from '@superset-ui/core';
import { ExportStatus, StreamingProgress } from './StreamingExportModal';
import { getFilenameFromResponse } from 'src/utils/export';
import { makeUrl } from 'src/utils/navigationUtils';
import { applicationRoot } from 'src/utils/getBootstrapData';

interface UseStreamingExportOptions {
  onComplete?: (downloadUrl: string | undefined, filename: string) => void;
  onError?: (error: string) => void;
}

type StreamingExportPayload = JsonObject;

type StreamingExportSource = 'chart' | 'sqllab';

interface StreamingExportParams {
  /**
   * The API endpoint URL for the export request.
   *
   * URLs should be prefixed with the application root at the call site using
   * `makeUrl()` from `src/utils/navigationUtils`. This ensures proper handling
   * for subdirectory deployments (e.g., /superset/api/v1/...).
   *
   * A defensive guard (`ensureUrlPrefix`) will apply the prefix if missing,
   * but callers should not rely on this fallback behavior.
   */
  url: string;
  payload: StreamingExportPayload;
  filename?: string;
  exportType: 'csv' | 'xlsx';
  exportSource?: StreamingExportSource;
  expectedRows?: number;
  target?: PreparedExportTarget;
}

const NEWLINE_BYTE = 10; // '\n' character code
const BLOB_FALLBACK_MAX_BYTES = 256 * 1024 * 1024;
const TASK_POLL_INTERVAL_MS = 1000;
const STREAM_ERROR_MARKER = '__STREAM_ERROR__:';

interface WritableFileStream {
  write(data: Uint8Array): Promise<void>;
  close(): Promise<void>;
  abort(reason?: unknown): Promise<void>;
}

interface WritableFileHandle {
  readonly name: string;
  createWritable(): Promise<WritableFileStream>;
}

interface WritableDirectoryHandle {
  getFileHandle(
    name: string,
    options?: { create?: boolean },
  ): Promise<WritableFileHandle>;
  removeEntry(name: string): Promise<void>;
}

interface DirectoryPickerWindow extends Window {
  showDirectoryPicker?: (options: {
    id: string;
    mode: 'readwrite';
    startIn: 'downloads';
  }) => Promise<WritableDirectoryHandle>;
}

export type PreparedExportTarget =
  { kind: 'directory'; handle: WritableDirectoryHandle } | { kind: 'blob' };

interface ArtifactExportResponse {
  task_uuid: string;
  status_url: string;
  artifact_url: string;
}

interface ExportSinkResult {
  downloadUrl?: string;
  filename?: string;
  savedDirectly: boolean;
}

interface ExportSink {
  write(chunk: Uint8Array): Promise<void>;
  close(): Promise<ExportSinkResult>;
  abort(reason?: unknown): Promise<void>;
}

/**
 * Ensures URL has the application root prefix for subdirectory deployments.
 * Applies makeUrl to relative paths that don't already include the app root.
 * This guards against callers forgetting to prefix URLs when using native fetch.
 */
const ensureUrlPrefix = (url: string): string => {
  const appRoot = applicationRoot();
  // Protocol-relative URLs (//example.com/...) should pass through unchanged
  if (url.startsWith('//')) {
    return url;
  }
  // Absolute URLs (http:// or https://) should pass through unchanged
  if (url.match(/^https?:\/\//)) {
    return url;
  }
  // Relative URLs without leading slash (e.g., "api/v1/...") need normalization
  // Add leading slash and apply prefix
  if (!url.startsWith('/')) {
    return makeUrl(`/${url}`);
  }
  // If no app root configured, return as-is
  if (!appRoot) {
    return url;
  }
  // If URL already has the app root prefix, return as-is
  // Use strict check to avoid false positives with sibling paths (e.g., /app2 when appRoot is /app)
  // Also handle query strings and hashes (e.g., /superset?foo=1 or /superset#hash)
  if (
    url === appRoot ||
    url.startsWith(`${appRoot}/`) ||
    url.startsWith(`${appRoot}?`) ||
    url.startsWith(`${appRoot}#`)
  ) {
    return url;
  }
  // Apply prefix via makeUrl
  return makeUrl(url);
};

const createFetchRequest = async (
  _url: string,
  payload: StreamingExportPayload,
  filename: string | undefined,
  _exportType: string,
  exportSource: StreamingExportSource | undefined,
  expectedRows: number | undefined,
  signal: AbortSignal,
): Promise<RequestInit> => {
  const headers: Record<string, string> = {
    'Content-Type': 'application/x-www-form-urlencoded',
  };

  const guestToken = SupersetClient.getGuestToken();
  const isGuestTokenChartExport =
    Boolean(guestToken) &&
    exportSource === 'chart' &&
    !('client_id' in payload);

  // Embedded guest sessions cannot fetch CSRF tokens. Guest chart exports are
  // safe because chart data is CSRF-exempt and auth is carried by guest_token.
  if (!isGuestTokenChartExport) {
    const csrfToken = await SupersetClient.getCSRFToken();
    if (csrfToken) {
      headers['X-CSRFToken'] = csrfToken;
    }
  }

  const formParams: Record<string, string> = {};

  if (filename) {
    formParams.filename = filename;
  }

  if (expectedRows !== undefined) {
    formParams.expected_rows = expectedRows.toString();
  }

  if (guestToken && isGuestTokenChartExport) {
    formParams.guest_token = guestToken;
  }

  if ('client_id' in payload) {
    // SQL Lab export - pass client_id directly
    formParams.client_id = String(payload.client_id);
  } else {
    // Chart export - wrap payload in form_data
    formParams.form_data = JSON.stringify(payload);
  }

  return {
    method: 'POST',
    headers,
    body: new URLSearchParams(formParams),
    signal,
    credentials: 'same-origin',
  };
};

const countNewlines = (value: Uint8Array): number =>
  value.filter(byte => byte === NEWLINE_BYTE).length;

const getExportMimeType = (exportType: string): string =>
  exportType === 'csv'
    ? 'text/csv;charset=utf-8'
    : 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';

const prepareExportTarget = async (
  _filename: string | undefined,
  _exportType: 'csv' | 'xlsx',
): Promise<PreparedExportTarget | null> => {
  const picker = (window as DirectoryPickerWindow).showDirectoryPicker;
  if (!picker) {
    return { kind: 'blob' };
  }

  try {
    // A directory grant is non-destructive. The concrete file is allocated only
    // after response headers establish its final CSV/XLSX/ZIP name.
    const handle = await picker.call(window, {
      id: 'superset-exports',
      mode: 'readwrite',
      startIn: 'downloads',
    });
    return { kind: 'directory', handle };
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') {
      return null;
    }
    if (error instanceof DOMException && error.name === 'SecurityError') {
      return { kind: 'blob' };
    }
    throw error;
  }
};

const safeExportFilename = (filename: string): string => {
  const basename = filename.split(/[\\/]/).pop() ?? '';
  const sanitized = basename.replace(/[\p{Cc}<>:"|?*]/gu, '_').trim();
  return sanitized && sanitized !== '.' && sanitized !== '..'
    ? sanitized
    : 'export';
};

const filenameWithSuffix = (filename: string, suffix: number): string => {
  if (suffix === 0) {
    return filename;
  }
  const extensionIndex = filename.lastIndexOf('.');
  return extensionIndex > 0
    ? `${filename.slice(0, extensionIndex)} (${suffix})${filename.slice(extensionIndex)}`
    : `${filename} (${suffix})`;
};

const createUniqueFileHandle = async (
  directory: WritableDirectoryHandle,
  filename: string,
): Promise<WritableFileHandle> => {
  const safeFilename = safeExportFilename(filename);
  for (let suffix = 0; suffix < 1000; suffix += 1) {
    const candidate = filenameWithSuffix(safeFilename, suffix);
    try {
      // eslint-disable-next-line no-await-in-loop
      await directory.getFileHandle(candidate);
    } catch (error) {
      if (error instanceof DOMException && error.name === 'NotFoundError') {
        // eslint-disable-next-line no-await-in-loop
        return directory.getFileHandle(candidate, { create: true });
      }
      if (error instanceof DOMException && error.name === 'TypeMismatchError') {
        continue;
      }
      throw error;
    }
  }
  throw new Error('Could not allocate a unique export filename');
};

const createExportSink = async (
  target: PreparedExportTarget,
  filename: string,
  contentType: string,
  declaredSize: number | undefined,
): Promise<ExportSink> => {
  if (target.kind === 'directory') {
    const fileHandle = await createUniqueFileHandle(target.handle, filename);
    const writable = await fileHandle.createWritable();
    return {
      write: chunk => writable.write(chunk),
      close: async () => {
        await writable.close();
        return { filename: fileHandle.name, savedDirectly: true };
      },
      abort: async reason => {
        try {
          await writable.abort(reason);
        } finally {
          await target.handle.removeEntry(fileHandle.name);
        }
      },
    };
  }

  if (declaredSize !== undefined && declaredSize > BLOB_FALLBACK_MAX_BYTES) {
    throw new Error(
      'This export is too large for this browser. Use a browser that supports saving streamed files.',
    );
  }

  const chunks: Uint8Array[] = [];
  let receivedLength = 0;
  return {
    write: async chunk => {
      if (receivedLength + chunk.length > BLOB_FALLBACK_MAX_BYTES) {
        throw new Error(
          'This export exceeded the browser memory fallback limit. Use a browser that supports saving streamed files.',
        );
      }
      // Retain only the visible bytes; a view can otherwise keep a much larger
      // response ArrayBuffer alive and defeat the fallback memory bound.
      chunks.push(chunk.slice());
      receivedLength += chunk.length;
    },
    close: async () => {
      const blob = new Blob(chunks, { type: contentType });
      chunks.length = 0;
      return { downloadUrl: URL.createObjectURL(blob), savedDirectly: false };
    },
    abort: async () => {
      chunks.length = 0;
    },
  };
};

const isArtifactExportResponse = (
  value: unknown,
): value is ArtifactExportResponse => {
  if (!value || typeof value !== 'object') {
    return false;
  }
  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.task_uuid === 'string' &&
    typeof candidate.status_url === 'string' &&
    typeof candidate.artifact_url === 'string'
  );
};

const waitForNextPoll = (signal: AbortSignal): Promise<void> =>
  new Promise((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout>;
    const onAbort = () => {
      clearTimeout(timer);
      reject(new DOMException('Export cancelled by user', 'AbortError'));
    };
    timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, TASK_POLL_INTERVAL_MS);
    signal.addEventListener('abort', onAbort, { once: true });
  });

const waitForArtifact = async (
  accepted: ArtifactExportResponse,
  signal: AbortSignal,
): Promise<Response> => {
  const activeStatuses = new Set(['pending', 'in_progress', 'aborting']);
  let shouldWait = false;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    if (shouldWait) {
      // eslint-disable-next-line no-await-in-loop
      await waitForNextPoll(signal);
    }
    shouldWait = true;
    if (signal.aborted) {
      throw new DOMException('Export cancelled by user', 'AbortError');
    }
    // eslint-disable-next-line no-await-in-loop
    const statusResponse = await fetch(ensureUrlPrefix(accepted.status_url), {
      credentials: 'same-origin',
      signal,
    });
    if (!statusResponse.ok) {
      throw new Error(
        `Export status failed: ${statusResponse.status} ${statusResponse.statusText}`,
      );
    }
    // eslint-disable-next-line no-await-in-loop
    const statusPayload = (await statusResponse.json()) as unknown;
    const status =
      statusPayload && typeof statusPayload === 'object'
        ? (statusPayload as Record<string, unknown>).status
        : undefined;
    if (status === 'success') {
      return fetch(ensureUrlPrefix(accepted.artifact_url), {
        credentials: 'same-origin',
        signal,
      });
    }
    if (typeof status !== 'string' || !activeStatuses.has(status)) {
      throw new Error(`Export task ended with status: ${String(status)}`);
    }
  }
};

export const useStreamingExport = (options: UseStreamingExportOptions = {}) => {
  const [progress, setProgress] = useState<StreamingProgress>({
    rowsProcessed: 0,
    totalRows: undefined,
    totalSize: 0,
    speed: 0,
    mbPerSecond: 0,
    elapsedTime: 0,
    status: ExportStatus.STREAMING,
  });
  const [retryCount, setRetryCount] = useState(0);
  const abortControllerRef = useRef<AbortController | null>(null);
  const activeTaskUuidRef = useRef<string | null>(null);
  const executionIdRef = useRef(0);
  const lastExportParamsRef = useRef<StreamingExportParams | null>(null);
  const currentBlobUrlRef = useRef<string | null>(null);
  const isExportingRef = useRef(false);

  const updateProgress = useCallback((updates: Partial<StreamingProgress>) => {
    setProgress(prev => ({ ...prev, ...updates }));
  }, []);

  const executeExport = useCallback(
    async (params: StreamingExportParams) => {
      const {
        url,
        payload,
        filename,
        exportType,
        exportSource,
        expectedRows,
        target = { kind: 'blob' },
      } = params;
      if (isExportingRef.current) {
        return;
      }
      isExportingRef.current = true;
      executionIdRef.current += 1;
      const executionId = executionIdRef.current;

      const abortController = new AbortController();
      abortControllerRef.current = abortController;
      let sink: ExportSink | null = null;
      const updateCurrentProgress = (updates: Partial<StreamingProgress>) => {
        if (executionIdRef.current === executionId) {
          updateProgress(updates);
        }
      };
      const isSqlLabExport =
        exportSource === 'sqllab' || 'client_id' in payload;

      updateCurrentProgress({
        rowsProcessed: 0,
        totalRows: expectedRows,
        totalSize: 0,
        speed: 0,
        mbPerSecond: 0,
        elapsedTime: 0,
        status: ExportStatus.STREAMING,
        filename,
      });

      try {
        const fetchOptions = await createFetchRequest(
          url,
          payload,
          filename,
          exportType,
          exportSource,
          expectedRows,
          abortController.signal,
        );
        // Guard: ensure URL has app root prefix for subdirectory deployments
        const prefixedUrl = ensureUrlPrefix(url);
        let response = await fetch(prefixedUrl, fetchOptions);

        if (!response.ok) {
          throw new Error(
            `Export failed: ${response.status} ${response.statusText}`,
          );
        }

        if (response.status === 202) {
          const acceptedPayload = (await response.json()) as unknown;
          if (!isArtifactExportResponse(acceptedPayload)) {
            throw new Error('Export service returned an invalid task response');
          }
          if (executionIdRef.current !== executionId) {
            SupersetClient.post({
              endpoint: `/api/v1/task/${acceptedPayload.task_uuid}/cancel`,
              jsonPayload: {},
            }).catch(() => undefined);
            throw new DOMException('Export cancelled by user', 'AbortError');
          }
          activeTaskUuidRef.current = acceptedPayload.task_uuid;
          response = await waitForArtifact(
            acceptedPayload,
            abortController.signal,
          );
          if (!response.ok) {
            throw new Error(
              `Artifact download failed: ${response.status} ${response.statusText}`,
            );
          }
        }

        if (!response.body) {
          throw new Error('Response body is not available for streaming');
        }

        const defaultFilename = `export.${exportType}`;
        const serverFilename = safeExportFilename(
          getFilenameFromResponse(response, defaultFilename),
        );

        const reader = response.body.getReader();
        const contentLength = response.headers.get('Content-Length');
        const declaredSize = contentLength
          ? Number.parseInt(contentLength, 10)
          : undefined;
        const responseContentType =
          response.headers.get('Content-Type') || getExportMimeType(exportType);
        const isCsvResponse = responseContentType
          .toLowerCase()
          .includes('text/csv');
        sink = await createExportSink(
          target,
          serverFilename,
          responseContentType,
          Number.isFinite(declaredSize) ? declaredSize : undefined,
        );
        let receivedLength = 0;
        let rowsProcessed = 0;
        const markerDecoder = new TextDecoder();
        let markerTail = '';

        // eslint-disable-next-line no-constant-condition
        while (true) {
          // eslint-disable-next-line no-await-in-loop
          const { done, value } = await reader.read();

          if (done) {
            break;
          }

          if (abortController.signal.aborted) {
            throw new DOMException('Export cancelled by user', 'AbortError');
          }

          if (isSqlLabExport && isCsvResponse) {
            const markerText =
              markerTail + markerDecoder.decode(value, { stream: true });
            const markerIndex = markerText.indexOf(STREAM_ERROR_MARKER);
            if (markerIndex >= 0) {
              const errorMessage = markerText
                .slice(markerIndex + STREAM_ERROR_MARKER.length)
                .trim();
              throw new Error(
                errorMessage || 'Export failed. Please try again.',
              );
            }
            markerTail = markerText.slice(-STREAM_ERROR_MARKER.length);
          }

          // eslint-disable-next-line no-await-in-loop
          await sink.write(value);
          receivedLength += value.length;

          // Count newlines using filter (more efficient than loop)
          // Note: This counts all newlines, including those within quoted CSV fields.
          // For an exact row count, server should send row count in response headers.
          if (isCsvResponse) {
            rowsProcessed += countNewlines(value);
          }

          // Update progress based on rows processed
          updateCurrentProgress({
            status: ExportStatus.STREAMING,
            rowsProcessed,
            totalRows: expectedRows,
            totalSize: receivedLength,
            filename: serverFilename,
          });
        }

        const sinkResult = await sink.close();
        sink = null;
        const completedFilename = sinkResult.filename || serverFilename;
        if (executionIdRef.current !== executionId) {
          if (sinkResult.downloadUrl) {
            URL.revokeObjectURL(sinkResult.downloadUrl);
          }
          return;
        }
        if (sinkResult.downloadUrl) {
          if (currentBlobUrlRef.current) {
            URL.revokeObjectURL(currentBlobUrlRef.current);
          }
          currentBlobUrlRef.current = sinkResult.downloadUrl;
        }

        updateCurrentProgress({
          status: ExportStatus.COMPLETED,
          downloadUrl: sinkResult.downloadUrl,
          filename: completedFilename,
          savedDirectly: sinkResult.savedDirectly,
        });

        if (executionIdRef.current === executionId) {
          options.onComplete?.(sinkResult.downloadUrl, completedFilename);
        }
      } catch (error) {
        if (sink) {
          await sink.abort(error).catch(() => undefined);
        }
        if (executionIdRef.current !== executionId) {
          return;
        }
        const errorMessage =
          error instanceof Error ? error.message : 'Unknown error occurred';

        if (
          (error instanceof DOMException && error.name === 'AbortError') ||
          errorMessage.includes('cancelled') ||
          errorMessage.includes('aborted')
        ) {
          updateCurrentProgress({
            status: ExportStatus.CANCELLED,
          });
        } else {
          updateCurrentProgress({
            status: ExportStatus.ERROR,
            error: errorMessage,
          });
          options.onError?.(errorMessage);
        }
      } finally {
        if (executionIdRef.current === executionId) {
          isExportingRef.current = false;
          activeTaskUuidRef.current = null;
          abortControllerRef.current = null;
        }
      }
    },
    [updateProgress, options],
  );

  const startExport = useCallback(
    async (params: StreamingExportParams) => {
      if (isExportingRef.current) {
        return;
      }

      setRetryCount(0);
      const target =
        params.target ||
        (await prepareExportTarget(params.filename, params.exportType));
      if (!target) {
        return;
      }
      const preparedParams = { ...params, target };
      lastExportParamsRef.current = preparedParams;

      updateProgress({
        rowsProcessed: 0,
        totalRows: params.expectedRows,
        totalSize: 0,
        speed: 0,
        mbPerSecond: 0,
        elapsedTime: 0,
        status: ExportStatus.STREAMING,
        filename: params.filename,
      });

      executeExport(preparedParams);
    },
    [updateProgress, executeExport],
  );

  const retryExport = useCallback(() => {
    if (!lastExportParamsRef.current) {
      return;
    }

    if (isExportingRef.current) {
      return;
    }

    setRetryCount(0);
    executeExport(lastExportParamsRef.current);
  }, [executeExport]);

  const cancelExport = useCallback(() => {
    const taskUuid = activeTaskUuidRef.current;
    activeTaskUuidRef.current = null;
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      updateProgress({
        status: ExportStatus.CANCELLED,
      });
    }
    if (taskUuid) {
      SupersetClient.post({
        endpoint: `/api/v1/task/${taskUuid}/cancel`,
        jsonPayload: {},
      }).catch(() => undefined);
    }
  }, [updateProgress]);

  const prepareExport = useCallback(
    (filename: string | undefined, exportType: 'csv' | 'xlsx') =>
      prepareExportTarget(filename, exportType),
    [],
  );

  const resetExport = useCallback(() => {
    cancelExport();
    executionIdRef.current += 1;
    if (currentBlobUrlRef.current) {
      URL.revokeObjectURL(currentBlobUrlRef.current);
      currentBlobUrlRef.current = null;
    }

    isExportingRef.current = false;
    activeTaskUuidRef.current = null;
    abortControllerRef.current = null;
    setProgress({
      rowsProcessed: 0,
      totalRows: undefined,
      totalSize: 0,
      speed: 0,
      mbPerSecond: 0,
      elapsedTime: 0,
      status: ExportStatus.STREAMING,
    });
  }, [cancelExport]);

  // Cleanup blob URL on unmount to prevent memory leak
  useEffect(
    () => () => {
      executionIdRef.current += 1;
      abortControllerRef.current?.abort();
      if (activeTaskUuidRef.current) {
        SupersetClient.post({
          endpoint: `/api/v1/task/${activeTaskUuidRef.current}/cancel`,
          jsonPayload: {},
        }).catch(() => undefined);
      }
      activeTaskUuidRef.current = null;
      abortControllerRef.current = null;
      isExportingRef.current = false;
      if (currentBlobUrlRef.current) {
        URL.revokeObjectURL(currentBlobUrlRef.current);
        currentBlobUrlRef.current = null;
      }
    },
    [],
  );

  return {
    progress,
    isExporting: isExportingRef.current,
    retryCount,
    prepareExport,
    startExport,
    cancelExport,
    resetExport,
    retryExport,
  };
};
