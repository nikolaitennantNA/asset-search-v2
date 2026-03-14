# #!/usr/bin/env python3
# """
# Async Crawl with Wait - SDK Example

# This script demonstrates async crawling with automatic polling (wait=True).
# The SDK automatically polls until the job completes and returns the results.

# Usage:
#     python 03_async_crawl_sdk.py

# Requirements:
#     pip install crawl4ai-cloud
# """

# import asyncio
# import io
# import json
# import zipfile

# import httpx
# from crawl4ai_cloud import AsyncWebCrawler, CrawlResult

# # Configuration
# API_KEY = (
#     "sk_live_NXjPO8z9bzbqK3p_LrrFTUgYAEabXSHQl_saE5IA3Ss"  # Replace with your API key
# )


# async def main():
#     """Create an async crawl job and wait for completion."""
#     async with AsyncWebCrawler(api_key=API_KEY) as crawler:
#         # URLs to crawl (can be more than 10 for async)
#         urls = [
#             "https://example.com",
#             "https://httpbin.org/html",
#             "https://httpbin.org/json",
#             "https://httpbin.org/robots.txt",
#         ]

#         print(f"Creating async job for {len(urls)} URLs...")

#         job = await crawler.run_many(urls)
#         print(f"Job {job.id} started")

#         # Wait for completion, then download results
#         job = await crawler.run_many(urls, wait=True)
#         if job.is_complete:
#             url = await crawler.download_url(job.id)


# if __name__ == "__main__":
#     asyncio.run(main())


import asyncio
from pathlib import Path
import zipfile
import httpx

from crawl4ai_cloud import AsyncWebCrawler, CrawlResult

# Configuration
API_KEY = (
    "sk_live_NXjPO8z9bzbqK3p_LrrFTUgYAEabXSHQl_saE5IA3Ss"  # Replace with your API key
)


async def download_and_extract_zip(zip_url: str, extract_to: str = ".") -> Path:
    """
    Download a ZIP from a presigned URL and extract it.

    Args:
        zip_url: Presigned URL returned by crawler.download_url(job.id)
        extract_to: Directory where files should be extracted.
                    "." means current working directory.

    Returns:
        Path to the extraction directory.
    """
    extract_dir = Path(extract_to).resolve() / "results"
    extract_dir.mkdir(parents=True, exist_ok=True)

    zip_path = extract_dir.parent / "results.zip"

    async with httpx.AsyncClient(follow_redirects=True, timeout=120.0) as client:
        response = await client.get(zip_url)
        response.raise_for_status()
        zip_path.write_bytes(response.content)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    # Optional: remove the zip after extraction
    zip_path.unlink(missing_ok=True)

    return extract_dir


async def main():
    """Create an async crawl job, wait for completion, then download results."""
    async with AsyncWebCrawler(api_key=API_KEY) as crawler:
        urls = [
            "https://example.com",
            "https://httpbin.org/html",
            "https://httpbin.org/json",
            "https://httpbin.org/robots.txt",
        ]

        print(f"Creating async job for {len(urls)} URLs...")

        job = await crawler.run_many(urls, wait=True)
        print(f"Job {job.id} finished: complete={job.is_complete}")

        if job.is_complete:
            zip_url = await crawler.download_url(job.id)
            saved_to = await download_and_extract_zip(zip_url, extract_to=".")
            print(f"Results extracted to: {saved_to}")
        else:
            print("Job did not complete.")


if __name__ == "__main__":
    asyncio.run(main())
