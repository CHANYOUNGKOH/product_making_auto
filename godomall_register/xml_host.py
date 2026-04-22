# -*- coding: utf-8 -*-
"""
XML 호스팅 - Cloudflare R2 업로드/삭제
고도몰 API는 data_url 파라미터로 XML을 호스팅한 URL을 받아 fetch함.
R2에 XML 업로드 → 공개 URL 반환 → API 호출 후 정리(삭제)
"""

import logging
import io

import boto3
from botocore.config import Config as BotoConfig

from .config import (
    R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
    R2_BUCKET_NAME, R2_PUBLIC_URL, R2_ENDPOINT_URL, R2_XML_PREFIX,
)

logger = logging.getLogger(__name__)


class XmlHost:
    def __init__(self):
        self._client = None

    def _get_client(self):
        if self._client is None:
            self._client = boto3.client(
                "s3",
                endpoint_url=R2_ENDPOINT_URL,
                aws_access_key_id=R2_ACCESS_KEY_ID,
                aws_secret_access_key=R2_SECRET_ACCESS_KEY,
                config=BotoConfig(
                    signature_version="s3v4",
                    retries={"max_attempts": 3, "mode": "adaptive"},
                    max_pool_connections=64,
                ),
                region_name="auto",
            )
        return self._client

    def upload(self, xml_content: str, filename: str) -> str:
        """
        XML 문자열을 R2에 업로드하고 공개 URL 반환
        """
        client = self._get_client()
        key = f"{R2_XML_PREFIX}/{filename}"

        body = xml_content.encode("utf-8")
        client.upload_fileobj(
            io.BytesIO(body),
            R2_BUCKET_NAME,
            key,
            ExtraArgs={"ContentType": "application/xml; charset=utf-8"},
        )

        public_url = f"{R2_PUBLIC_URL}/{key}"
        logger.info(f"XML uploaded: {public_url}")
        return public_url

    def cleanup(self, filenames: list):
        """사용 후 XML 파일 삭제"""
        client = self._get_client()
        for fn in filenames:
            key = f"{R2_XML_PREFIX}/{fn}"
            try:
                client.delete_object(Bucket=R2_BUCKET_NAME, Key=key)
                logger.debug(f"XML deleted: {key}")
            except Exception as e:
                logger.warning(f"XML 삭제 실패 {key}: {e}")
