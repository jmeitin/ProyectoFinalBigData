console.log("Hello world");

document.querySelector('.menu-btn').addEventListener('click', ()=>{
    console.log('click')
    document.querySelector('.nav-menu').classList.toggle('show')
})


ScrollReveal().reveal('.showcase');
ScrollReveal().reveal('.news-cards', {delay: 100});
ScrollReveal().reveal('.cards-banner-one', {delay: 100});
ScrollReveal().reveal('.cards-banner-two', {delay: 100});