#!/usr/bin/env bash
# Installation et mise à jour de Téléinfo Linky.
#
# Usage :
#   curl -fsSL https://github.com/hleroy/teleinfo-linky/releases/latest/download/install.sh | bash
#
# Variables facultatives :
#   LINKY_DIR       répertoire d'installation (par défaut : ~/linky)
#   LINKY_VERSION   version à installer, par exemple 1.1.0 (par défaut : la dernière)
#   LINKY_URL_BASE  adresse des fichiers de la version (par défaut : la page GitHub de la version)
#
# Le script ne modifie jamais un fichier config.yml existant. Les commandes qui demandent les
# droits administrateur (sudo) sont annoncées avant d'être exécutées.

set -euo pipefail

DEPOT="https://github.com/hleroy/teleinfo-linky"
ARCHIVE="teleinfo-linky.tar.gz"
LINKY_DIR="${LINKY_DIR:-$HOME/linky}"
LINKY_DIR="${LINKY_DIR%/}"
if [ -n "${LINKY_VERSION:-}" ]; then
    LINKY_URL_BASE="${LINKY_URL_BASE:-$DEPOT/releases/download/v$LINKY_VERSION}"
else
    LINKY_URL_BASE="${LINKY_URL_BASE:-$DEPOT/releases/latest/download}"
fi
SERVICE_DIR="$HOME/.config/systemd/user"
VENV="$LINKY_DIR/.venv"

etape() { printf '\n==> %s\n' "$*"; }
info() { printf '    %s\n' "$*"; }
erreur() { printf 'ERREUR : %s\n' "$*" >&2; exit 1; }

# Exécute une commande avec sudo, ou affiche la commande à lancer si sudo n'est pas disponible.
# Renvoie 1 dans ce dernier cas.
administrateur() {
    if command -v sudo >/dev/null 2>&1; then
        info "sudo $*"
        sudo "$@"
    else
        info "sudo n'est pas disponible. Lancez en tant qu'administrateur : $*"
        return 1
    fi
}

# Tout le script est dans une fonction : avec « curl | bash », bash lit ainsi le script en entier
# avant de l'exécuter, et une commande qui lit l'entrée standard ne peut pas en consommer la suite.
main() {
    # --- Vérifications -----------------------------------------------------------------------------

    etape "Vérification du système"
    [ "$(id -u)" -ne 0 ] || erreur "lancez ce script avec votre utilisateur habituel, pas en root."
    # LINKY_DIR est écrit tel quel dans le service systemd : chemin absolu, sans espace ni
    # caractère interprété par sed (| & \) ou par systemd (%).
    case "$LINKY_DIR" in
        /*) ;;
        *) erreur "LINKY_DIR doit être un chemin absolu : $LINKY_DIR" ;;
    esac
    case "$LINKY_DIR" in
        *[[:space:]\|\&\\%]*) erreur "LINKY_DIR ne doit contenir ni espace ni l'un des caractères | & \\ % : $LINKY_DIR" ;;
    esac
    command -v curl >/dev/null 2>&1 || erreur "curl est nécessaire."
    command -v python3 >/dev/null 2>&1 || erreur "python3 est nécessaire : sudo apt install python3"
    python3 -c 'import sys; sys.exit(sys.version_info < (3, 9))' \
        || erreur "Python 3.9 ou plus récent est nécessaire (version actuelle : $(python3 -V 2>&1))."
    info "$(python3 -V 2>&1)"

    # Les paquets installés par pip dans ~/.local (version 1.0.0) sont ignorés : seuls comptent
    # les paquets Debian et ceux de l'environnement virtuel.
    export PYTHONNOUSERSITE=1

    # PyYAML et pySerial sont pris dans les paquets Debian, déjà compilés : sur un Raspberry Pi
    # Zero, pip devrait sinon les télécharger, voire les compiler, ce qui est très lent.
    PAQUETS=()
    python3 -c 'import ensurepip, venv' >/dev/null 2>&1 || PAQUETS+=(python3-venv)
    python3 -c 'import yaml' >/dev/null 2>&1 || PAQUETS+=(python3-yaml)
    python3 -c 'import serial' >/dev/null 2>&1 || PAQUETS+=(python3-serial)
    if [ "${#PAQUETS[@]}" -gt 0 ]; then
        etape "Installation des paquets système : ${PAQUETS[*]}"
        if ! { administrateur apt-get update </dev/null \
                && administrateur apt-get install -y "${PAQUETS[@]}" </dev/null; }; then
            python3 -c 'import ensurepip, venv' >/dev/null 2>&1 || erreur "python3-venv est nécessaire."
            info "Paquets non installés : pip téléchargera les dépendances manquantes."
        fi
    fi

    # --- Téléchargement ----------------------------------------------------------------------------

    TMP="$(mktemp -d)"
    trap 'rm -rf "$TMP"' EXIT

    etape "Téléchargement depuis $LINKY_URL_BASE"
    curl -fsSL -o "$TMP/$ARCHIVE" "$LINKY_URL_BASE/$ARCHIVE"
    curl -fsSL -o "$TMP/$ARCHIVE.sha256" "$LINKY_URL_BASE/$ARCHIVE.sha256"
    (cd "$TMP" && sha256sum -c "$ARCHIVE.sha256") || erreur "la somme de contrôle de l'archive est incorrecte."

    mkdir "$TMP/extrait"
    tar -xzf "$TMP/$ARCHIVE" -C "$TMP/extrait"
    SOURCE="$(find "$TMP/extrait" -mindepth 1 -maxdepth 1 -type d -name 'teleinfo-linky-*' | head -n 1)"
    [ -n "$SOURCE" ] || erreur "l'archive ne contient pas le répertoire attendu."
    info "Version : ${SOURCE##*/teleinfo-linky-}"

    # --- Installation ------------------------------------------------------------------------------

    etape "Installation dans $LINKY_DIR"
    MISE_A_JOUR=false
    [ -f "$LINKY_DIR/config.yml" ] && MISE_A_JOUR=true
    mkdir -p "$LINKY_DIR"
    rm -rf "$LINKY_DIR/teleinfo_linky"
    cp -a "$SOURCE/." "$LINKY_DIR/"

    etape "Installation des dépendances Python dans $VENV"
    info "Cette étape peut prendre plusieurs minutes sur un Raspberry Pi Zero."
    # L'environnement virtuel voit les paquets Debian (--system-site-packages). Un environnement
    # créé sans cette option, par une version précédente de ce script, est recréé.
    if [ -x "$VENV/bin/python" ] \
            && ! grep -qx 'include-system-site-packages = true' "$VENV/pyvenv.cfg" 2>/dev/null; then
        info "Recréation de l'environnement virtuel pour utiliser les paquets Debian."
        rm -rf "$VENV"
    fi
    [ -x "$VENV/bin/python" ] || python3 -m venv --system-site-packages "$VENV"
    # Le pip de Debian 11 (20.3) construirait le paquet avec le setuptools de Debian, trop ancien,
    # au lieu de celui demandé par pyproject.toml : il est d'abord mis à jour.
    if ! "$VENV/bin/python" -c 'import sys, pip; sys.exit(int(pip.__version__.split(".")[0]) < 22)'; then
        info "Mise à jour de pip, trop ancien"
        "$VENV/bin/pip" install --disable-pip-version-check --upgrade pip
    fi
    # Depuis la copie temporaire : les fichiers de construction (build/, *.egg-info) y restent.
    "$VENV/bin/pip" install --disable-pip-version-check "${SOURCE}[influxdb]"
    info "$("$VENV/bin/teleinfo-linky" --version)"

    if [ "$MISE_A_JOUR" = true ]; then
        info "Fichier config.yml existant conservé."
    else
        cp "$LINKY_DIR/config.example.yml" "$LINKY_DIR/config.yml"
        info "Fichier config.yml créé à partir de config.example.yml."
    fi

    # --- Droits sur le port série ------------------------------------------------------------------

    RECONNEXION=false
    if ! id -nG | tr ' ' '\n' | grep -qx dialout; then
        etape "Ajout de $USER au groupe dialout (accès au port série)"
        if administrateur usermod -a -G dialout "$USER"; then
            RECONNEXION=true
        fi
    fi

    # --- Service systemd ---------------------------------------------------------------------------

    etape "Installation du service systemd utilisateur"
    mkdir -p "$SERVICE_DIR"
    cp "$LINKY_DIR/linky.service" "$SERVICE_DIR/linky.service"
    if [ "$LINKY_DIR" != "$HOME/linky" ]; then
        sed -i "s|%h/linky|$LINKY_DIR|g" "$SERVICE_DIR/linky.service"
    fi

    SYSTEMD=false
    if systemctl --user show-environment >/dev/null 2>&1; then
        SYSTEMD=true
        systemctl --user daemon-reload
        systemctl --user enable linky
        if [ "$(loginctl show-user "$USER" -p Linger --value 2>/dev/null || true)" != "yes" ]; then
            info "Démarrage automatique du service sans session ouverte :"
            administrateur loginctl enable-linger "$USER" || true
        fi
        if [ "$MISE_A_JOUR" = true ]; then
            systemctl --user restart linky
            info "Service linky redémarré."
        fi
    else
        info "systemd utilisateur n'est pas disponible : service copié mais pas activé."
    fi

    # --- Fin ---------------------------------------------------------------------------------------

    etape "Installation terminée"
    if [ "$MISE_A_JOUR" = false ]; then
        info "1. Editez $LINKY_DIR/config.yml"
        if [ "$SYSTEMD" = true ]; then
            info "2. Démarrez le service : systemctl --user start linky"
        fi
    fi
    info "Journaux du service : journalctl --user-unit linky -f"
    if [ "$RECONNEXION" = true ]; then
        info "Déconnectez-vous puis reconnectez-vous pour que l'accès au port série prenne effet."
    fi
}

main "$@"
