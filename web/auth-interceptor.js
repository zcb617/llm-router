(function () {
  if (window.__llmAuthInterceptorInstalled === true) {
    return;
  }

  const nativeFetch = window.fetch.bind(window);
  let authExpiredPromise = null;

  // 统一处理登录 Token 过期，确保并发请求只提示一次并跳转登录页。
  window.handleAuthExpiredToken = function handleAuthExpiredToken() {
    if (authExpiredPromise) {
      return authExpiredPromise;
    }

    authExpiredPromise = new Promise(() => {
      localStorage.removeItem('llm_token');
      localStorage.removeItem('llm_email');
      localStorage.removeItem('llm_user_id');
      window.alert('登录状态已过期，请重新登录。');
      window.location.replace('/web/login.html');
    });

    return authExpiredPromise;
  };

  // 判断响应是否为后端返回的登录 Token 过期错误。
  async function isExpiredTokenResponse(response) {
    if (response.status !== 401) {
      return false;
    }

    try {
      const data = await response.clone().json();
      return data && data.error === 'Unauthorized: invalid or expired token';
    } catch (error) {
      return false;
    }
  }

  window.fetch = async function (...args) {
    const response = await nativeFetch(...args);
    if (!(await isExpiredTokenResponse(response))) {
      return response;
    }

    await window.handleAuthExpiredToken();
    return new Promise(() => {});
  };

  window.__llmAuthInterceptorInstalled = true;
})();
