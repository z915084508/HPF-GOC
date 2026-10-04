# Hoppie credential rotation before self-hosted activation

Treat the credential formerly committed in `.env` as exposed. Removing this file does not remove it from Git history, clones, or caches.

1. Identify every active GOC sender and every consumer of the credential. Do not launch a second sender during validation.
2. Arrange a replacement credential through the Hoppie account owner using the provider's supported process. Do not paste credentials into issues, PRs, logs, or chat.
3. Store the replacement only in the HISPAFLY server's restricted secrets directory or a secret manager. Pass it to the worker at runtime; never bake it into the image or commit it.
4. Verify the application reads the server-provided value and cannot fall back to a committed credential. Perform controlled validation with message delivery disabled until the sender handover is ready.
5. At the agreed handover, stop the old GOC sender, enable the new sender, validate a controlled message, and invalidate the exposed credential. If immediate revocation is possible, coordinate its impact with all existing consumers.
6. Confirm the old credential is invalid and that restart behavior, persistent worker state, and log redaction are correct before marking rotation complete.

Rotation is pending until replacement and revocation are verified. The repository change alone is not credential rotation. History cleanup, if requested, is a separate coordinated operation; do not force-push as part of deployment.
