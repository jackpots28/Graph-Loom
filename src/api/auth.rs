//! Authentication and authorization module for Graph-Loom API
//!
//! Provides JWT validation, API key authentication, and Role-Based Access Control (RBAC).

#![allow(dead_code)]

use std::collections::HashSet;
use serde::{Deserialize, Serialize};

/// Permissions that can be granted to users
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    ReadNodes,
    WriteNodes,
    DeleteNodes,
    ReadRelationships,
    WriteRelationships,
    DeleteRelationships,
    ExecuteQueries,
    AdminSettings,
}

/// User roles with predefined permission sets
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Role {
    /// Read-only access to nodes and relationships
    #[default]
    Viewer,
    /// Can read and write nodes/relationships, execute queries
    Editor,
    /// Full access including delete and admin settings
    Admin,
}

impl Role {
    /// Get the set of permissions for this role
    pub fn permissions(&self) -> HashSet<Permission> {
        match self {
            Role::Viewer => {
                let mut perms = HashSet::new();
                perms.insert(Permission::ReadNodes);
                perms.insert(Permission::ReadRelationships);
                perms
            }
            Role::Editor => {
                let mut perms = HashSet::new();
                perms.insert(Permission::ReadNodes);
                perms.insert(Permission::WriteNodes);
                perms.insert(Permission::ReadRelationships);
                perms.insert(Permission::WriteRelationships);
                perms.insert(Permission::ExecuteQueries);
                perms
            }
            Role::Admin => {
                let mut perms = HashSet::new();
                perms.insert(Permission::ReadNodes);
                perms.insert(Permission::WriteNodes);
                perms.insert(Permission::DeleteNodes);
                perms.insert(Permission::ReadRelationships);
                perms.insert(Permission::WriteRelationships);
                perms.insert(Permission::DeleteRelationships);
                perms.insert(Permission::ExecuteQueries);
                perms.insert(Permission::AdminSettings);
                perms
            }
        }
    }

    /// Check if this role has a specific permission
    pub fn has_permission(&self, perm: Permission) -> bool {
        self.permissions().contains(&perm)
    }
}

/// Authentication provider type
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum AuthProvider {
    /// Simple API key authentication (existing behavior)
    #[default]
    ApiKey,
    /// JWT-based authentication (for future OAuth2/OIDC integration)
    Jwt,
}

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Whether authentication is enabled
    pub enabled: bool,
    /// The authentication provider to use
    pub provider: AuthProvider,
    /// For JWT: the issuer URL (e.g., OAuth2 provider)
    pub jwt_issuer: Option<String>,
    /// For JWT: expected audience claim
    pub jwt_audience: Option<String>,
    /// For JWT: secret key for HS256 validation (for simple setups)
    pub jwt_secret: Option<String>,
    /// Default role for authenticated users without explicit role claim
    pub default_role: Role,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: AuthProvider::ApiKey,
            jwt_issuer: None,
            jwt_audience: None,
            jwt_secret: None,
            default_role: Role::Editor,
        }
    }
}

/// Represents an authenticated user/session
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// User identifier (API key name, JWT subject, etc.)
    pub identity: String,
    /// The user's role
    pub role: Role,
    /// Cached permissions for quick lookup
    permissions: HashSet<Permission>,
}

impl AuthContext {
    /// Create a new auth context with the given identity and role
    pub fn new(identity: String, role: Role) -> Self {
        let permissions = role.permissions();
        Self { identity, role, permissions }
    }

    /// Create an anonymous context (for unauthenticated requests when auth is disabled)
    pub fn anonymous() -> Self {
        Self::new("anonymous".to_string(), Role::Admin)
    }

    /// Check if this context has a specific permission
    pub fn has_permission(&self, perm: Permission) -> bool {
        self.permissions.contains(&perm)
    }

    /// Check if this context can execute a query (based on query content)
    pub fn can_execute_query(&self, query: &str) -> bool {
        let upper = query.to_uppercase();
        
        // Check for mutating operations
        if upper.contains("CREATE") || upper.contains("MERGE") || upper.contains("SET ") {
            if !self.has_permission(Permission::WriteNodes) && !self.has_permission(Permission::WriteRelationships) {
                return false;
            }
        }
        
        if upper.contains("DELETE") || upper.contains("REMOVE") {
            if !self.has_permission(Permission::DeleteNodes) && !self.has_permission(Permission::DeleteRelationships) {
                return false;
            }
        }
        
        // Read operations require at least read permission
        if upper.contains("MATCH") || upper.contains("RETURN") {
            if !self.has_permission(Permission::ReadNodes) && !self.has_permission(Permission::ReadRelationships) {
                return false;
            }
        }
        
        // CALL procedures require ExecuteQueries permission
        if upper.contains("CALL ") {
            if !self.has_permission(Permission::ExecuteQueries) {
                return false;
            }
        }
        
        true
    }
}

/// Result of authentication attempt
#[derive(Debug)]
pub enum AuthResult {
    /// Authentication successful
    Success(AuthContext),
    /// Authentication failed (invalid credentials)
    InvalidCredentials,
    /// Authentication required but not provided
    MissingCredentials,
    /// Token expired
    Expired,
    /// Auth is disabled, proceed as anonymous
    Disabled,
}

/// Validate an API key and return auth context
pub fn validate_api_key(provided_key: Option<&str>, required_key: Option<&str>, config: &AuthConfig) -> AuthResult {
    if !config.enabled {
        return AuthResult::Disabled;
    }
    
    match (provided_key, required_key) {
        (_, None) => AuthResult::Disabled, // No key required
        (None, Some(_)) => AuthResult::MissingCredentials,
        (Some(provided), Some(required)) => {
            if provided == required {
                AuthResult::Success(AuthContext::new("api_key_user".to_string(), config.default_role))
            } else {
                AuthResult::InvalidCredentials
            }
        }
    }
}

/// Simple JWT claims structure (for basic JWT validation)
#[derive(Debug, Deserialize)]
pub struct JwtClaims {
    /// Subject (user identifier)
    pub sub: String,
    /// Expiration time (Unix timestamp)
    pub exp: Option<i64>,
    /// Issued at (Unix timestamp)
    pub iat: Option<i64>,
    /// Role claim (optional, uses default if not present)
    pub role: Option<String>,
}

/// Validate a JWT token (basic validation without external dependencies)
/// For production use, consider using the `jsonwebtoken` crate
pub fn validate_jwt_simple(token: &str, config: &AuthConfig) -> AuthResult {
    if !config.enabled {
        return AuthResult::Disabled;
    }
    
    // Basic JWT structure validation (header.payload.signature)
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return AuthResult::InvalidCredentials;
    }
    
    // Decode payload (base64url)
    let payload = match base64_url_decode(parts[1]) {
        Some(p) => p,
        None => return AuthResult::InvalidCredentials,
    };
    
    // Parse claims
    let claims: JwtClaims = match serde_json::from_slice(&payload) {
        Ok(c) => c,
        Err(_) => return AuthResult::InvalidCredentials,
    };
    
    // Check expiration
    if let Some(exp) = claims.exp {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        if now > exp {
            return AuthResult::Expired;
        }
    }
    
    // Determine role from claims or use default
    let role = claims.role
        .as_ref()
        .and_then(|r| match r.to_lowercase().as_str() {
            "admin" => Some(Role::Admin),
            "editor" => Some(Role::Editor),
            "viewer" => Some(Role::Viewer),
            _ => None,
        })
        .unwrap_or(config.default_role);
    
    AuthResult::Success(AuthContext::new(claims.sub, role))
}

/// Simple base64url decoder (without padding)
fn base64_url_decode(input: &str) -> Option<Vec<u8>> {
    // Convert base64url to standard base64
    let mut s = input.replace('-', "+").replace('_', "/");
    
    // Add padding if needed
    match s.len() % 4 {
        2 => s.push_str("=="),
        3 => s.push('='),
        _ => {}
    }
    
    // Decode using a simple implementation
    // For production, use the `base64` crate
    decode_base64(&s)
}

fn decode_base64(input: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    
    let mut output = Vec::new();
    let mut buffer = 0u32;
    let mut bits = 0;
    
    for c in input.bytes() {
        if c == b'=' {
            break;
        }
        let val = ALPHABET.iter().position(|&x| x == c)? as u32;
        buffer = (buffer << 6) | val;
        bits += 6;
        
        if bits >= 8 {
            bits -= 8;
            output.push((buffer >> bits) as u8);
            buffer &= (1 << bits) - 1;
        }
    }
    
    Some(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_permissions() {
        assert!(Role::Viewer.has_permission(Permission::ReadNodes));
        assert!(!Role::Viewer.has_permission(Permission::WriteNodes));
        
        assert!(Role::Editor.has_permission(Permission::ReadNodes));
        assert!(Role::Editor.has_permission(Permission::WriteNodes));
        assert!(!Role::Editor.has_permission(Permission::DeleteNodes));
        
        assert!(Role::Admin.has_permission(Permission::ReadNodes));
        assert!(Role::Admin.has_permission(Permission::DeleteNodes));
        assert!(Role::Admin.has_permission(Permission::AdminSettings));
    }

    #[test]
    fn test_auth_context_query_permissions() {
        let viewer = AuthContext::new("test".to_string(), Role::Viewer);
        assert!(viewer.can_execute_query("MATCH (n) RETURN n"));
        assert!(!viewer.can_execute_query("CREATE (n:Test)"));
        
        let editor = AuthContext::new("test".to_string(), Role::Editor);
        assert!(editor.can_execute_query("MATCH (n) RETURN n"));
        assert!(editor.can_execute_query("CREATE (n:Test)"));
        assert!(!editor.can_execute_query("DELETE n"));
        
        let admin = AuthContext::new("test".to_string(), Role::Admin);
        assert!(admin.can_execute_query("DELETE n"));
    }

    #[test]
    fn test_api_key_validation() {
        let config = AuthConfig {
            enabled: true,
            default_role: Role::Editor,
            ..Default::default()
        };
        
        // Valid key
        match validate_api_key(Some("secret"), Some("secret"), &config) {
            AuthResult::Success(ctx) => assert_eq!(ctx.role, Role::Editor),
            _ => panic!("Expected success"),
        }
        
        // Invalid key
        match validate_api_key(Some("wrong"), Some("secret"), &config) {
            AuthResult::InvalidCredentials => {}
            _ => panic!("Expected invalid credentials"),
        }
        
        // Missing key
        match validate_api_key(None, Some("secret"), &config) {
            AuthResult::MissingCredentials => {}
            _ => panic!("Expected missing credentials"),
        }
    }

    #[test]
    fn test_base64_decode() {
        let decoded = base64_url_decode("SGVsbG8gV29ybGQ").unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), "Hello World");
    }
}
