"use strict";

(() => {
  const $ = (id) => document.getElementById(id);
  const maxFileSize = 10 * 1024 * 1024;
  const state = { userID: "", file: null, previewURL: "", avatars: [], generation: 0, uploading: false, deleting: new Set(), timer: null, loading: false };
  let notificationTimer;

  function notify(message, error = false) {
    clearTimeout(notificationTimer);
    $("notification-text").textContent = message;
    $("notification").classList.toggle("error", error);
    $("notification").hidden = false;
    notificationTimer = setTimeout(() => { $("notification").hidden = true; }, error ? 10000 : 5000);
  }

  function readableError(body, fallback) {
    const translations = {
      "Avatar not found": "Аватарка не найдена. Обновите галерею.",
      "Forbidden": "Удалять можно только свои аватарки.",
      "File too large": "Файл слишком большой. Максимальный размер — 10 МБ.",
      "Invalid file format": "Выберите изображение JPEG, PNG или WebP.",
      "Invalid request": "Проверьте ID пользователя и выбранное изображение.",
      "Internal server error": "Сервис временно недоступен. Попробуйте ещё раз.",
      "X-User-ID header is required": "Укажите ID пользователя."
    };
    return translations[body && body.error] || fallback;
  }

  async function api(path, options = {}) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30000);
    try {
      const response = await fetch(path, { ...options, signal: controller.signal });
      if (response.headers.get("X-App-Mode") === "demo") $("demo-banner").hidden = false;
      const body = response.status === 204 ? null : await response.json();
      if (!response.ok) throw new Error(readableError(body, `Не удалось выполнить запрос (${response.status}).`));
      return body;
    } catch (error) {
      if (error.name === "AbortError") throw new Error("Сервис долго не отвечает. Повторите запрос.");
      if (error instanceof TypeError) throw new Error("Нет связи с сервером. Проверьте подключение.");
      if (error instanceof SyntaxError) throw new Error("Сервис вернул неожиданный ответ. Попробуйте ещё раз.");
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }

  function bytes(value) {
    return value >= 1024 * 1024 ? `${(value / 1024 / 1024).toFixed(1)} МБ` : `${Math.max(1, Math.round(value / 1024))} КБ`;
  }

  function imageURL(id, size) {
    return `/api/v1/avatars/${encodeURIComponent(id)}${size ? `?size=${encodeURIComponent(size)}` : ""}`;
  }

  function renderGallery() {
    $("avatar-count").textContent = String(state.avatars.length);
    $("gallery-summary").textContent = `Коллекция пользователя ${state.userID}`;
    $("empty-state").hidden = state.avatars.length > 0;
    $("polling-note").hidden = !state.avatars.some((avatar) => avatar.status === "processing");
    const fragment = document.createDocumentFragment();
    for (const avatar of state.avatars) {
      const card = $("avatar-card").content.cloneNode(true);
      const original = imageURL(avatar.id);
      const ready = avatar.status === "completed";
      card.querySelector(".avatar-image").href = original;
      const image = card.querySelector("img");
      image.src = ready ? imageURL(avatar.id, "300x300") : original;
      image.alt = `Аватарка: ${avatar.file_name || "изображение"}`;
      image.addEventListener("error", () => {


        if (image.getAttribute("src") !== original) image.src = original;
      });
      const status = card.querySelector(".status");
      status.textContent = ready ? "Готово" : avatar.status === "failed" ? "Ошибка обработки" : "Обрабатывается";
      status.classList.add(ready ? "completed" : avatar.status === "failed" ? "failed" : "processing");
      const name = card.querySelector(".avatar-name");
      name.textContent = avatar.file_name;
      name.title = avatar.file_name;
      const dimensions = avatar.dimensions || {};
      card.querySelector(".avatar-meta").textContent = `${dimensions.width || "?"} × ${dimensions.height || "?"} · ${bytes(avatar.size || 0)}`;
      card.querySelector(".original-link").href = original;
      for (const [selector, size] of [[".small-link", "100x100"], [".medium-link", "300x300"]]) {
        const link = card.querySelector(selector);
        link.href = imageURL(avatar.id, size);
        link.hidden = !ready;
        link.setAttribute("aria-label", `Открыть миниатюру ${size}`);
      }
      card.querySelector(".processing-error").hidden = avatar.status !== "failed";
      const deleteButton = card.querySelector(".delete-avatar");
      deleteButton.disabled = state.deleting.has(avatar.id);
      deleteButton.textContent = state.deleting.has(avatar.id) ? "Удаляем…" : "Удалить";
      deleteButton.setAttribute("aria-label", `Удалить ${avatar.file_name}`);
      deleteButton.addEventListener("click", () => deleteAvatar(avatar));
      fragment.appendChild(card);
    }
    $("gallery").replaceChildren(fragment);
  }

  function scheduleRefresh() {
    clearTimeout(state.timer);
    if (state.avatars.some((avatar) => avatar.status === "processing")) {
      state.timer = setTimeout(() => {
        if (document.visibilityState === "visible") loadGallery(false);
        else scheduleRefresh();
      }, 2000);
    }
  }

  async function loadGallery(showError = true) {
    if (!state.userID || state.loading) return;
    const userID = state.userID;
    const generation = state.generation;
    state.loading = true;
    $("refresh").disabled = true;
    try {
      const avatars = await api(`/api/v1/users/${encodeURIComponent(userID)}/avatars`, { cache: "no-store" });
      if (generation !== state.generation) return;
      if (!Array.isArray(avatars)) throw new Error("Сервер вернул неожиданный ответ.");
      state.avatars = avatars;
      renderGallery();
    } catch (error) {
      if (generation === state.generation && showError) notify(error.message, true);
    } finally {
      state.loading = false;
      $("refresh").disabled = false;
      if (generation === state.generation) scheduleRefresh();
      else loadGallery();
    }
  }

  function selectUser(userID, updateURL = true) {
    userID = userID.trim();
    if (!userID || userID.length > 255 || /[^\x21-\x7e]|[/\\]/u.test(userID)) {
      notify("ID до 255 символов: латиница, цифры и знаки без пробелов и слешей.", true);
      return;
    }
    state.userID = userID;
    state.generation += 1;
    state.avatars = [];
    clearTimeout(state.timer);
    $("user-id").value = userID;
    try { localStorage.setItem("gophprofile.userID", userID); } catch (_) {  }
    if (updateURL) history.replaceState(null, "", `/web/gallery/${encodeURIComponent(userID)}`);
    renderGallery();
    loadGallery();
  }

  function selectFile(file) {
    if (!file) return;
    if (file.size > maxFileSize) { resetFile(); notify("Файл слишком большой. Максимальный размер — 10 МБ.", true); return; }
    if (!file.size || (file.type && !["image/jpeg", "image/png", "image/webp"].includes(file.type))) {
      resetFile();
      notify("Выберите изображение JPEG, PNG или WebP.", true);
      return;
    }
    if (state.previewURL) URL.revokeObjectURL(state.previewURL);
    state.file = file;
    state.previewURL = URL.createObjectURL(file);
    $("preview").src = state.previewURL;
    $("selected-name").textContent = file.name;
    $("selected-size").textContent = bytes(file.size);
    $("drop-empty").hidden = true;
    $("preview-wrap").hidden = false;
    $("reset-file").hidden = false;
  }

  function resetFile() {
    if (state.previewURL) URL.revokeObjectURL(state.previewURL);
    state.file = null;
    state.previewURL = "";
    $("avatar-file").value = "";
    $("avatar-file").required = true;
    $("preview").removeAttribute("src");
    $("preview-wrap").hidden = true;
    $("drop-empty").hidden = false;
    $("reset-file").hidden = true;
  }

  function setUploading(value) {
    state.uploading = value;
    for (const id of ["upload-button", "avatar-file", "user-id", "open-user", "reset-file"]) $(id).disabled = value;
    $("upload-label").textContent = value ? "Загружаем…" : "Загрузить аватарку";
    $("upload-form").setAttribute("aria-busy", String(value));
  }

  async function upload(event) {
    event.preventDefault();
    if (state.uploading) return;
    if (!state.file) { notify("Сначала выберите изображение.", true); return; }
    if (!state.userID) { notify("Сначала укажите ID пользователя.", true); return; }
    const typedUserID = $("user-id").value.trim();
    if (typedUserID !== state.userID) {
      notify("Нажмите «Открыть», чтобы переключить пользователя перед загрузкой.", true);
      return;
    }
    setUploading(true);
    const form = new FormData();
    form.append("file", state.file);
    try {
      const avatar = await api("/api/v1/avatars", { method: "POST", headers: { "X-User-ID": state.userID }, body: form });
      state.generation += 1;
      state.avatars.unshift(avatar);
      renderGallery();
      scheduleRefresh();
      resetFile();
      notify("Аватарка загружена. Готовим миниатюры.");
      loadGallery(false);
    } catch (error) {
      notify(error.message, true);
    } finally { setUploading(false); }
  }

  async function deleteAvatar(avatar) {
    if (state.deleting.has(avatar.id)) return;
    const userID = state.userID;
    state.deleting.add(avatar.id);
    renderGallery();
    try {
      await api(`/api/v1/avatars/${encodeURIComponent(avatar.id)}`, { method: "DELETE", headers: { "X-User-ID": userID } });
      if (userID === state.userID) {
        state.generation += 1;
        state.avatars = state.avatars.filter((item) => item.id !== avatar.id);
        renderGallery();
        notify("Аватарка удалена.");
        loadGallery(false);
      }
    } catch (error) { notify(error.message, true); }
    finally {
      state.deleting.delete(avatar.id);
      if (userID === state.userID) renderGallery();
    }
  }

  async function health() {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 5000);
    try {
      const response = await fetch("/health", { cache: "no-store", signal: controller.signal });
      const body = await response.json();
      const mode = response.headers.get("X-App-Mode") || body.app_mode || body.mode;
      $("demo-banner").hidden = mode !== "demo";
      $("health-dot").className = response.ok ? "dot ok" : "dot error";
      $("health-label").textContent = response.ok ? "Сервис работает" : "Часть сервисов недоступна";
    } catch (_) {
      $("health-dot").className = "dot error";
      $("health-label").textContent = "Нет связи с сервером";
    } finally { clearTimeout(timeout); }
  }

  $("identity-form").addEventListener("submit", (event) => { event.preventDefault(); selectUser($("user-id").value); });
  $("upload-form").addEventListener("submit", upload);
  $("avatar-file").addEventListener("change", () => selectFile($("avatar-file").files[0]));
  $("reset-file").addEventListener("click", () => $("avatar-file").click());
  $("refresh").addEventListener("click", () => loadGallery());
  $("dismiss-notification").addEventListener("click", () => { $("notification").hidden = true; });
  $("dropzone").addEventListener("dragover", (event) => { event.preventDefault(); if (!state.uploading) $("dropzone").classList.add("dragging"); });
  $("dropzone").addEventListener("dragleave", () => $("dropzone").classList.remove("dragging"));
  $("dropzone").addEventListener("drop", (event) => {
    event.preventDefault();
    $("dropzone").classList.remove("dragging");
    if (state.uploading) return;
    const file = event.dataTransfer.files[0];
    selectFile(file);

    $("avatar-file").required = !state.file;
  });
  document.addEventListener("visibilitychange", () => { if (document.visibilityState === "visible") { loadGallery(false); health(); } });
  window.addEventListener("beforeunload", () => { if (state.previewURL) URL.revokeObjectURL(state.previewURL); });
  let userID = "demo-user";
  try { userID = localStorage.getItem("gophprofile.userID") || userID; } catch (_) {  }
  const route = location.pathname.match(/^\/web\/gallery\/([^/]+)\/?$/);
  if (route) { try { userID = decodeURIComponent(route[1]); } catch (_) {  } }
  selectUser(userID, false);
  health();
  setInterval(() => { if (document.visibilityState === "visible") health(); }, 15000);
})();
