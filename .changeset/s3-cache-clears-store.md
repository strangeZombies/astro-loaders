---
"@ascorbic/s3-media-loader": patch
---

Clear the Astro content store before applying cached or refreshed S3 media so deleted objects do not remain in the collection.
