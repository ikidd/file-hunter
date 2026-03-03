const ConfirmModal = {
    overlayEl: null,
    titleEl: null,
    textEl: null,
    submitEl: null,
    _resolve: null,

    init() {
        this.overlayEl = document.getElementById('confirm-modal');
        this.titleEl = document.getElementById('confirm-modal-title');
        this.textEl = document.getElementById('confirm-modal-text');
        this.submitEl = document.getElementById('confirm-modal-submit');

        document.getElementById('confirm-modal-cancel').addEventListener('click', () => this._finish(false));
        this.submitEl.addEventListener('click', () => this._finish(true));

        this.overlayEl.addEventListener('click', (e) => {
            if (e.target === this.overlayEl) this._finish(false);
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this.overlayEl.classList.contains('hidden')) {
                this._finish(false);
            }
        });
    },

    /** Show the modal and return a promise that resolves true (confirm) or false (cancel). */
    open({ title = 'Confirm', message, confirmLabel = 'OK' } = {}) {
        this.titleEl.textContent = title;
        this.textEl.textContent = message;
        this.submitEl.textContent = confirmLabel;
        this.overlayEl.classList.remove('hidden');
        return new Promise((resolve) => { this._resolve = resolve; });
    },

    _finish(result) {
        this.overlayEl.classList.add('hidden');
        if (this._resolve) {
            this._resolve(result);
            this._resolve = null;
        }
    },
};

export default ConfirmModal;
