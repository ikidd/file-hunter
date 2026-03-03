const Toast = {
    _container: null,

    _ensureContainer() {
        if (this._container) return;
        this._container = document.createElement('div');
        this._container.id = 'toast-container';
        document.body.appendChild(this._container);
    },

    success(msg) { this._show(msg, 'success'); },
    error(msg)   { this._show(msg, 'error'); },
    info(msg)    { this._show(msg, 'info'); },

    _show(message, level) {
        this._ensureContainer();

        const toast = document.createElement('div');
        toast.className = `toast toast-${level}`;
        toast.innerHTML = `
            <span class="toast-msg">${message}</span>
            <button class="toast-close">&times;</button>
        `;

        toast.querySelector('.toast-close').addEventListener('click', () => this._dismiss(toast));

        this._container.appendChild(toast);

        // Trigger slide-in on next frame
        requestAnimationFrame(() => toast.classList.add('toast-visible'));

        // Auto-dismiss after 4 seconds
        setTimeout(() => this._dismiss(toast), 4000);
    },

    _dismiss(toast) {
        if (toast.classList.contains('toast-dismissing')) return;
        toast.classList.add('toast-dismissing');
        toast.addEventListener('transitionend', () => toast.remove(), { once: true });
        // Fallback removal if transition doesn't fire
        setTimeout(() => toast.remove(), 400);
    },
};

export default Toast;
