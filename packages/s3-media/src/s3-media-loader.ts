import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import type { LoaderContext } from "astro/loaders";
import { ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import type { ListObjectsV2CommandOutput } from "@aws-sdk/client-s3";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { getLoaderProxyAgent } from "@ascorbic/loader-utils";
import type { MediaItem, S3LoaderOptions } from "./schema.js";

type ListedObject = {
	key: string;
	size?: number;
	lastModified?: string;
};

type CachedListing = {
	version: 2;
	fetchedAt: number;
	objects: ListedObject[];
};

type LoaderLogger = LoaderContext["logger"];
type S3LoaderContext = Pick<LoaderContext, "store" | "logger">;

interface S3MediaLoader {
	name: string;
	load: (context: S3LoaderContext) => Promise<void>;
}

const CACHE_DIR = path.join(process.cwd(), "src/content/content-metadata");
const CACHE_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const cachedClients = new Map<string, S3Client>();
const cachedListings = new Map<string, CachedListing>();
const pendingListings = new Map<string, Promise<CachedListing>>();
const failedListings = new Map<string, { retryAfter: number; error: unknown }>();

function fingerprint(values: unknown[]): string {
	return createHash("sha256").update(JSON.stringify(values)).digest("hex");
}

function getClientKey(options: S3LoaderOptions): string {
	return fingerprint([
		options.endpoint,
		options.region ?? "auto",
		options.accessKeyId,
		options.secretAccessKey,
		options.forcePathStyle ?? true,
	]);
}

function getListingCacheKey(options: S3LoaderOptions, listingPrefix: string): string {
	return fingerprint([
		getClientKey(options),
		options.bucket,
		listingPrefix,
	]);
}

function getListingCachePath(cacheKey: string): string {
	return path.join(CACHE_DIR, `s3-media-v2-${cacheKey}.json`);
}

function isListedObject(value: unknown): value is ListedObject {
	if (!value || typeof value !== "object") return false;
	const item = value as Record<string, unknown>;
	return (
		typeof item.key === "string" &&
		(item.size === undefined || (typeof item.size === "number" && Number.isFinite(item.size))) &&
		(item.lastModified === undefined ||
			(typeof item.lastModified === "string" && Number.isFinite(Date.parse(item.lastModified))))
	);
}

async function readListingCache(cacheKey: string): Promise<CachedListing | null> {
	try {
		const contents = await fs.readFile(getListingCachePath(cacheKey), "utf8");
		const parsed = JSON.parse(contents) as Partial<CachedListing>;
		if (
			parsed.version !== 2 ||
			typeof parsed.fetchedAt !== "number" ||
			!Array.isArray(parsed.objects) ||
			!parsed.objects.every(isListedObject)
		) {
			return null;
		}
		return parsed as CachedListing;
	} catch {
		return null;
	}
}

async function writeListingCache(cacheKey: string, listing: CachedListing): Promise<void> {
	const filePath = getListingCachePath(cacheKey);
	const temporaryPath = `${filePath}.${process.pid}.${randomUUID()}.tmp`;
	try {
		await fs.mkdir(CACHE_DIR, { recursive: true });
		await fs.writeFile(temporaryPath, JSON.stringify(listing));
		await fs.rename(temporaryPath, filePath);
	} catch (error) {
		await fs.rm(temporaryPath, { force: true }).catch(() => undefined);
		console.error("Failed to write S3 media listing cache", error);
	}
}

function getOrCreateClient(options: S3LoaderOptions): S3Client {
	const cacheKey = getClientKey(options);
	const cachedClient = cachedClients.get(cacheKey);
	if (cachedClient) return cachedClient;

	const proxyAgent = getLoaderProxyAgent();
	const client = new S3Client({
		endpoint: options.endpoint,
		region: options.region ?? "auto",
		credentials: {
			accessKeyId: options.accessKeyId,
			secretAccessKey: options.secretAccessKey,
		},
		forcePathStyle: options.forcePathStyle ?? true,
		...(proxyAgent
			? {
					requestHandler: new NodeHttpHandler({
						httpAgent: proxyAgent,
						httpsAgent: proxyAgent,
					}),
				}
			: {}),
	});

	cachedClients.set(cacheKey, client);
	return client;
}

async function* listAllObjects(
	client: S3Client,
	bucket: string,
	prefix: string,
	maxKeys: number,
): AsyncGenerator<NonNullable<ListObjectsV2CommandOutput["Contents"]>[number]> {
	let continuationToken: string | undefined;

	do {
		const response = await client.send(
			new ListObjectsV2Command({
				Bucket: bucket,
				Prefix: prefix,
				MaxKeys: maxKeys,
				ContinuationToken: continuationToken,
			}),
		);

		for (const item of response.Contents ?? []) yield item;
		continuationToken = response.NextContinuationToken;
	} while (continuationToken);
}

async function fetchListing(
	options: S3LoaderOptions,
	listingPrefix: string,
): Promise<ListedObject[]> {
	const objects: ListedObject[] = [];
	const client = getOrCreateClient(options);
	for await (const item of listAllObjects(
		client,
		options.bucket,
		listingPrefix,
		options.maxKeys ?? 1000,
	)) {
		if (!item.Key) continue;
		objects.push({
			key: item.Key,
			...(item.Size !== undefined ? { size: item.Size } : {}),
			...(item.LastModified ? { lastModified: item.LastModified.toISOString() } : {}),
		});
	}
	return objects;
}

async function loadSharedListing(
	options: S3LoaderOptions,
	listingPrefix: string,
	logger: LoaderLogger,
): Promise<CachedListing> {
	const cacheKey = getListingCacheKey(options, listingPrefix);
	const memoryCache = cachedListings.get(cacheKey);
	const diskCache = memoryCache ?? (await readListingCache(cacheKey));
	const now = Date.now();

	if (diskCache && now - diskCache.fetchedAt < CACHE_TTL_MS) {
		cachedListings.set(cacheKey, diskCache);
		return diskCache;
	}
	const previousFailure = failedListings.get(cacheKey);
	if (previousFailure && now < previousFailure.retryAfter) {
		if (diskCache) return diskCache;
		throw previousFailure.error;
	}

	const pending = pendingListings.get(cacheKey);
	if (pending) return pending;

	const request = (async () => {
		try {
			const objects = await fetchListing(options, listingPrefix);
			const listing: CachedListing = {
				version: 2,
				fetchedAt: Date.now(),
				objects,
			};
			cachedListings.set(cacheKey, listing);
			failedListings.delete(cacheKey);
			await writeListingCache(cacheKey, listing);
			logger.info(`Fetched ${listing.objects.length} S3 object records for the shared listing`);
			return listing;
		} catch (error) {
			failedListings.set(cacheKey, { retryAfter: Date.now() + 60_000, error });
			if (diskCache) {
				cachedListings.set(cacheKey, diskCache);
				logger.info(`S3 listing failed; using stale shared cache with ${diskCache.objects.length} objects`);
				return diskCache;
			}
			throw error;
		}
	})();

	pendingListings.set(cacheKey, request);
	try {
		return await request;
	} finally {
		if (pendingListings.get(cacheKey) === request) pendingListings.delete(cacheKey);
	}
}

function buildUrl(publicBaseUrl: string, key: string): string {
	const base = publicBaseUrl.endsWith("/") ? publicBaseUrl.slice(0, -1) : publicBaseUrl;
	const normalizedKey = key.startsWith("/") ? key.slice(1) : key;
	return `${base}/${normalizedKey}`;
}

function toMediaItem(
	object: ListedObject,
	options: S3LoaderOptions,
	allowedExtensions: Set<string>,
): MediaItem | null {
	const { key } = object;
	const name = key.slice(key.lastIndexOf("/") + 1);
	const lastDotIndex = name.lastIndexOf(".");
	const ext = lastDotIndex === -1 ? "" : name.slice(lastDotIndex).toLowerCase();
	if (!ext || !allowedExtensions.has(ext)) return null;

	return {
		id: key,
		name,
		ext,
		url: buildUrl(options.publicBaseUrl, key),
		...(object.size !== undefined ? { size: object.size } : {}),
		...(object.lastModified ? { lastModified: new Date(object.lastModified) } : {}),
	};
}

function getLegacyCacheFile(options: S3LoaderOptions): string {
	const legacyKey = `${options.bucket}:${options.prefix ?? ""}`.replace(/[:/]/g, "_");
	return path.join(CACHE_DIR, `s3-media-${legacyKey}.json`);
}

async function readLegacyCache(options: S3LoaderOptions): Promise<MediaItem[] | null> {
	try {
		const contents = await fs.readFile(getLegacyCacheFile(options), "utf8");
		const parsed = JSON.parse(contents) as unknown;
		if (!Array.isArray(parsed)) return null;

		return parsed.flatMap((value): MediaItem[] => {
			if (!value || typeof value !== "object") return [];
			const item = value as Partial<MediaItem>;
			if (typeof item.id !== "string" || typeof item.name !== "string" || typeof item.ext !== "string") {
				return [];
			}
			if (options.prefix && !item.id.startsWith(options.prefix)) return [];
			return [{
				...item,
				url: buildUrl(options.publicBaseUrl, item.id),
				...(typeof item.lastModified === "string"
					? { lastModified: new Date(item.lastModified) }
					: {}),
			}] as MediaItem[];
		});
	} catch {
		return null;
	}
}

async function writeItemsToStore(store: LoaderContext["store"], items: MediaItem[]): Promise<void> {
	await store.clear();
	for (const item of items) {
		store.set({
			id: item.id,
			data: item,
			rendered: { html: "" },
		});
	}
}

export function s3Loader(options: S3LoaderOptions): S3MediaLoader {
	const prefix = options.prefix ?? "";
	const listingPrefix = options.sharedPrefix ?? prefix;
	if (!prefix.startsWith(listingPrefix)) {
		throw new Error("S3 loader prefix must be inside the configured sharedPrefix");
	}

	const allowedExtensions = new Set(
		(options.extensions ?? [".mp3", ".flac", ".wav", ".mp4", ".webm", ".ogg"]).map((value) =>
			value.toLowerCase(),
		),
	);

	return {
		name: "s3",
		async load({ store, logger }: S3LoaderContext) {
			try {
				const listing = await loadSharedListing(options, listingPrefix, logger);
				const items = listing.objects.flatMap((object) => {
					if (!object.key.startsWith(prefix)) return [];
					const item = toMediaItem(object, options, allowedExtensions);
					return item ? [item] : [];
				});

				await writeItemsToStore(store, items);
				logger.info(`Loaded ${items.length} media items from the shared S3 listing`);
			} catch (error) {
				const cachedItems = await readLegacyCache(options);
				const legacyItems = cachedItems?.filter((item) => allowedExtensions.has(item.ext.toLowerCase()));
				if (legacyItems) {
					await writeItemsToStore(store, legacyItems);
					logger.info(`S3 listing failed; loaded ${legacyItems.length} media items from the legacy cache`);
					return;
				}

				const message = error instanceof Error ? error.message : String(error);
				logger.error(`Error loading media from S3: ${message}`);
				throw error;
			}
		},
	};
}
