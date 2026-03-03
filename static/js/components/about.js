import API from '../api.js';

const About = {
    _modal: null,
    _content: null,

    init() {
        this._modal = document.getElementById('about-modal');
        this._content = document.getElementById('about-content');

        document.getElementById('about-close').addEventListener('click', () => this.close());
        this._modal.addEventListener('click', (e) => {
            if (e.target === this._modal) this.close();
        });
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this._modal.classList.contains('hidden')) {
                this.close();
            }
        });
    },

    async open() {
        let version = '…';
        let pro = false;
        const res = await API.get('/api/version');
        if (res.ok) { version = res.data.version; pro = res.data.pro; }

        this._content.innerHTML = `
            <div class="about-info">
                <div class="about-version">File Hunter v${this._esc(version)}${pro ? ' (Pro)' : ''}</div>
                <p class="about-desc">File cataloging and deduplication tool for managing large removable and archival storage.</p>
                <p class="about-links">
                    <a href="https://github.com/mhandley/file-hunter" target="_blank" rel="noopener">GitHub</a>
                </p>
                <p class="about-copyright">&copy; 2026 <a href="https://zenlogic.co.uk" target="_blank" rel="noopener">Zen Logic Ltd.</a></p>
            </div>
        `;
        this._modal.classList.remove('hidden');
    },

    close() {
        this._modal.classList.add('hidden');
    },

    _esc(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    },
};

export default About;
