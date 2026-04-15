from __future__ import annotations

from typing import Any

from client import PolymarketClient
from models import PublicProfile, ResolvedTrader, SearchProfile
from utils import (
    AmbiguousResolutionError,
    ResolutionError,
    is_wallet_address,
    normalize_text,
)


class ProfileResolver:
    """Resolve a Polymarket wallet or public profile name into a wallet address."""

    def __init__(self, client: PolymarketClient) -> None:
        self.client = client

    def resolve(self, wallet: str | None, name: str | None) -> ResolvedTrader:
        if bool(wallet) == bool(name):
            raise ResolutionError("Provide exactly one of --wallet or --name.")
        if wallet:
            return self._resolve_wallet(wallet)
        if name is None:
            raise ResolutionError(
                "A profile name is required when --wallet is not provided."
            )
        return self._resolve_name(name)

    def _resolve_wallet(self, wallet: str) -> ResolvedTrader:
        if not is_wallet_address(wallet):
            raise ResolutionError(f"Invalid wallet address: {wallet}")
        normalized_wallet = wallet.casefold()
        profile = self.client.get_public_profile(normalized_wallet)
        return self._build_resolved_trader(
            input_type="wallet",
            input_value=wallet,
            wallet=normalized_wallet,
            profile=profile,
        )

    def _resolve_name(self, name: str) -> ResolvedTrader:
        candidates = [
            profile
            for profile in self.client.search_profiles(name)
            if profile.proxy_wallet
        ]
        if not candidates:
            raise ResolutionError(f"No public Polymarket profile matched {name!r}.")

        ranked = sorted(
            (
                (self._score_candidate(name, candidate), candidate)
                for candidate in candidates
            ),
            key=lambda item: item[0],
            reverse=True,
        )
        best_score, best_candidate = ranked[0]
        same_score = [candidate for score, candidate in ranked if score == best_score]

        if best_score < 60 or len(same_score) > 1:
            raise AmbiguousResolutionError(
                f"Profile name {name!r} is ambiguous. Please choose a wallet explicitly.",
                candidates=[
                    self._candidate_payload(candidate) for _, candidate in ranked[:10]
                ],
            )

        next_score = ranked[1][0] if len(ranked) > 1 else 0
        if best_score < next_score + 20:
            raise AmbiguousResolutionError(
                f"Profile name {name!r} matched multiple similar public profiles.",
                candidates=[
                    self._candidate_payload(candidate) for _, candidate in ranked[:10]
                ],
            )

        profile = self.client.get_public_profile(best_candidate.proxy_wallet or "")
        return self._build_resolved_trader(
            input_type="name",
            input_value=name,
            wallet=(best_candidate.proxy_wallet or "").casefold(),
            profile=profile,
            fallback_name=best_candidate.name,
            fallback_pseudonym=best_candidate.pseudonym,
        )

    def _build_resolved_trader(
        self,
        *,
        input_type: str,
        input_value: str,
        wallet: str,
        profile: PublicProfile | None,
        fallback_name: str | None = None,
        fallback_pseudonym: str | None = None,
    ) -> ResolvedTrader:
        if profile is None:
            return ResolvedTrader(
                input_type=input_type,
                input_value=input_value,
                wallet=wallet,
                display_name=fallback_name or fallback_pseudonym or wallet,
                profile_name=fallback_name,
                pseudonym=fallback_pseudonym,
                profile_found=False,
            )

        display_name = profile.name or profile.pseudonym or wallet
        return ResolvedTrader(
            input_type=input_type,
            input_value=input_value,
            wallet=wallet,
            display_name=display_name,
            profile_name=profile.name,
            pseudonym=profile.pseudonym,
            bio=profile.bio,
            verified_badge=bool(profile.verified_badge),
            created_at=profile.created_at,
            profile_found=True,
        )

    @staticmethod
    def _score_candidate(query: str, candidate: SearchProfile) -> int:
        normalized_query = normalize_text(query)
        names = [candidate.name or "", candidate.pseudonym or ""]
        best = 0
        for value in names:
            normalized_value = normalize_text(value)
            if not normalized_value:
                continue
            if normalized_value == normalized_query:
                best = max(best, 100)
            elif normalized_value.startswith(normalized_query):
                best = max(best, 85)
            elif normalized_query in normalized_value:
                best = max(best, 70)
            elif normalized_value.replace(" ", "") == normalized_query.replace(" ", ""):
                best = max(best, 90)
        return best

    @staticmethod
    def _candidate_payload(candidate: SearchProfile) -> dict[str, Any]:
        return {
            "wallet": candidate.proxy_wallet,
            "name": candidate.name,
            "pseudonym": candidate.pseudonym,
            "bio": candidate.bio,
        }
