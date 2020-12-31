const releaseNotesGenOptions = {
    "preset": "conventionalcommits",
};

const gitOptions = {
    "assets": [
    ],
    "message": "chore(release): ${nextRelease.version} [skip ci]\n\n${nextRelease.notes}"
};

module.exports = {
    "dryRun": false,
    "branches": [
        '+([0-9])?(.{+([0-9]),x}).x',
        'master',
        'next',
        'next-major',
        {name: 'beta', prerelease: true},
        {name: 'alpha', prerelease: true}
    ],
    "plugins": [
        "@semantic-release/commit-analyzer",
        [
            "@semantic-release/release-notes-generator",
            releaseNotesGenOptions
        ],
        "@semantic-release/changelog",
        "@semantic-release/github",
        [
            "@semantic-release/git",
            gitOptions
        ]
    ]
};
