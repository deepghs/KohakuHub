import { defineStore, acceptHMRUpdate } from "pinia";
import { ref, computed } from "vue";
import { verifyAdminToken } from "@/utils/api";

/**
 * Admin store - manages admin token in memory only (no persistence)
 */
export const useAdminStore = defineStore("admin", () => {
  // State - token is kept in memory only, not persisted
  const token = ref("");
  const isAuthenticated = ref(false);

  // Computed
  const hasToken = computed(() => !!token.value);

  // Actions
  async function login(adminToken) {
    // Verify token is valid before storing
    try {
      const valid = await verifyAdminToken(adminToken);
      if (valid) {
        token.value = adminToken;
        isAuthenticated.value = true;
        return true;
      } else {
        token.value = "";
        isAuthenticated.value = false;
        return false;
      }
    } catch (error) {
      token.value = "";
      isAuthenticated.value = false;
      throw error;
    }
  }

  function logout() {
    token.value = "";
    isAuthenticated.value = false;
    // Drop session-scoped UI flags so the next login gets a fresh
    // first-visit experience (e.g. the chain tester's auto-load
    // from system on first navigation — see fallback-sources.vue).
    try {
      sessionStorage.removeItem("khub_admin_chain_tester_draft_loaded_once");
    } catch (_e) {
      // sessionStorage blocked — nothing to clean up.
    }
  }

  return {
    token,
    isAuthenticated,
    hasToken,
    login,
    logout,
  };
});

// Hot-replace this store on edit instead of full-reloading the page.
// Without this, editing admin.js would full-reload and dump the in-memory
// token, making local development of auth-related code painful.
if (import.meta.hot) {
  import.meta.hot.accept(acceptHMRUpdate(useAdminStore, import.meta.hot));
}
