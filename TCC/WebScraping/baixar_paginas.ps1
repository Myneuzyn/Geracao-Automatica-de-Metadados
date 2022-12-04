$urls = Get-Content -Path "C:\Repositorios\TCC\WebScraping\urls.txt"

$lista_documentos_metadados = @()

foreach ($url in $urls) {

    $documento = [PSCustomObject]@{
        URL            = ''
        Titulo         = ''
        Autores        = ''
        DataPublicado  = ''
        PalavrasChaves = ''
        Resumo         = ''
    }

    $array_item = @()


    $documento.URL = $url

    $paginaWeb = Invoke-WebRequest -Uri $url

    #Dividir a pagina em um array de linhas
    $conteudo = $paginaWeb.Content.Split("`r`n")

    #DublinCore já classificados na página
    $hashtable_DC = @{
        Titulo         = ('<meta name="DC.title"');
        Autores        = ('<meta name="DC.creator"');
        DataPublicado  = ('<meta name="DCTERMS.issued"');
        PalavrasChaves = ('<meta name="DC.subject"')
        Resumo         = ('<meta name="DCTERMS.abstract"');
    }

    foreach ($DC in $hashtable_DC.GetEnumerator()) {
        $array_item.Clear()
        $linhas = $conteudo | Where-Object { $_.StartsWith($DC.Value) }

        if($DC.Value -eq '<meta name="DCTERMS.abstract"' -and !$linhas) {continue}

        foreach ($cada_linha in $linhas) {
            $item = $cada_linha.Substring($DC.Value.length + 10)
            $item = $item.Substring(0, $item.IndexOf('"'))
            $array_item += $item 
        }

        $documento.$($DC.Key) = $array_item | Sort-Object -Unique
    }

    $lista_documentos_metadados += $documento
}

$lista_documentos_metadados | ConvertTo-Json | Out-File "C:\Repositorios\TCC\WebScraping\lista_documentos.json"