use mantissa_protocol::server::{
    self, SessionBootstrapRejectionCode as WireSessionBootstrapRejectionCode,
};

/// Stable rejection reasons returned by peer session bootstrap RPCs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SessionBootstrapRejectionCode {
    UnknownSessionTicket,
    PeerNotRegistered,
    LocalNodeInactive,
    CredentialInvalid,
    IssuerMismatch,
    IssuerUnknown,
}

impl SessionBootstrapRejectionCode {
    /// # Description:
    ///
    /// Converts one internal rejection code to its wire enum representation.
    pub(crate) fn to_wire(self) -> WireSessionBootstrapRejectionCode {
        match self {
            Self::UnknownSessionTicket => WireSessionBootstrapRejectionCode::UnknownSessionTicket,
            Self::PeerNotRegistered => WireSessionBootstrapRejectionCode::PeerNotRegistered,
            Self::LocalNodeInactive => WireSessionBootstrapRejectionCode::LocalNodeInactive,
            Self::CredentialInvalid => WireSessionBootstrapRejectionCode::CredentialInvalid,
            Self::IssuerMismatch => WireSessionBootstrapRejectionCode::IssuerMismatch,
            Self::IssuerUnknown => WireSessionBootstrapRejectionCode::IssuerUnknown,
        }
    }

    /// # Description:
    ///
    /// Converts one wire rejection code into its internal representation.
    pub(crate) fn from_wire(code: WireSessionBootstrapRejectionCode) -> Self {
        match code {
            WireSessionBootstrapRejectionCode::UnknownSessionTicket => Self::UnknownSessionTicket,
            WireSessionBootstrapRejectionCode::PeerNotRegistered => Self::PeerNotRegistered,
            WireSessionBootstrapRejectionCode::LocalNodeInactive => Self::LocalNodeInactive,
            WireSessionBootstrapRejectionCode::CredentialInvalid => Self::CredentialInvalid,
            WireSessionBootstrapRejectionCode::IssuerMismatch => Self::IssuerMismatch,
            WireSessionBootstrapRejectionCode::IssuerUnknown => Self::IssuerUnknown,
        }
    }

    /// # Description:
    ///
    /// Returns whether this rejection proves a locally cached ticket cannot be reused.
    pub(crate) fn rejects_cached_ticket(self) -> bool {
        matches!(
            self,
            Self::UnknownSessionTicket | Self::PeerNotRegistered | Self::LocalNodeInactive
        )
    }

    /// # Description:
    ///
    /// Returns whether this rejection is expected during fast membership convergence.
    pub(crate) fn is_transient_convergence(self) -> bool {
        matches!(
            self,
            Self::UnknownSessionTicket
                | Self::PeerNotRegistered
                | Self::LocalNodeInactive
                | Self::IssuerUnknown
        )
    }

    /// Returns whether repeated session bootstrap attempts require retry backoff.
    pub(crate) fn requires_retry_backoff(self) -> bool {
        matches!(
            self,
            Self::PeerNotRegistered
                | Self::LocalNodeInactive
                | Self::CredentialInvalid
                | Self::IssuerMismatch
                | Self::IssuerUnknown
        )
    }

    /// # Description:
    ///
    /// Returns the stable log label for this rejection code.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::UnknownSessionTicket => "unknownSessionTicket",
            Self::PeerNotRegistered => "peerNotRegistered",
            Self::LocalNodeInactive => "localNodeInactive",
            Self::CredentialInvalid => "credentialInvalid",
            Self::IssuerMismatch => "issuerMismatch",
            Self::IssuerUnknown => "issuerUnknown",
        }
    }

    /// # Description:
    ///
    /// Returns the default human-facing detail for this rejection code.
    pub(crate) fn default_detail(self) -> &'static str {
        match self {
            Self::UnknownSessionTicket => "unknown session ticket",
            Self::PeerNotRegistered => "peer not registered",
            Self::LocalNodeInactive => "node is not an active cluster member",
            Self::CredentialInvalid => "credential invalid",
            Self::IssuerMismatch => "issuer mismatch for subject",
            Self::IssuerUnknown => "issuer unknown",
        }
    }
}

/// Typed peer session bootstrap rejection returned across the control plane.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SessionBootstrapRejection {
    pub(crate) code: SessionBootstrapRejectionCode,
    pub(crate) detail: String,
}

impl SessionBootstrapRejection {
    /// # Description:
    ///
    /// Builds one typed session bootstrap rejection with explicit diagnostic detail.
    pub(crate) fn new(code: SessionBootstrapRejectionCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    /// # Description:
    ///
    /// Builds one typed session bootstrap rejection with the code's default detail.
    pub(crate) fn with_default_detail(code: SessionBootstrapRejectionCode) -> Self {
        Self::new(code, code.default_detail())
    }

    /// # Description:
    ///
    /// Returns true when this rejection should remove the local cached session ticket.
    pub(crate) fn rejects_cached_ticket(&self) -> bool {
        self.code.rejects_cached_ticket()
    }

    /// Returns true when this rejection should cool down the next bootstrap attempt.
    pub(crate) fn requires_retry_backoff(&self) -> bool {
        self.code.requires_retry_backoff()
    }

    /// # Description:
    ///
    /// Returns true when this rejection should stay below warning level during convergence.
    pub(crate) fn is_transient_convergence(&self) -> bool {
        self.code.is_transient_convergence()
    }

    /// # Description:
    ///
    /// Returns one stable diagnostic string for telemetry and logs.
    pub(crate) fn summary(&self) -> String {
        format!("{}: {}", self.code.as_str(), self.detail)
    }

    /// # Description:
    ///
    /// Converts one wire rejection reader into a typed rejection.
    pub(crate) fn from_wire(
        reader: server::session_bootstrap_rejection::Reader<'_>,
    ) -> Result<Self, capnp::Error> {
        let code = SessionBootstrapRejectionCode::from_wire(reader.get_code()?);
        let detail = reader.get_detail()?.to_str()?.to_string();
        Ok(Self { code, detail })
    }

    /// # Description:
    ///
    /// Writes this typed rejection into a wire rejection builder.
    pub(crate) fn write_to(&self, mut builder: server::session_bootstrap_rejection::Builder<'_>) {
        builder.set_code(self.code.to_wire());
        builder.set_detail(&self.detail);
    }

    /// # Description:
    ///
    /// Converts this typed rejection to a Cap'n Proto exception for legacy RPCs.
    pub(crate) fn to_capnp_error(&self) -> capnp::Error {
        capnp::Error::failed(self.summary())
    }
}
